"""Discover filers (srNums) with new LM-10 activity since a known max rptId.

rptIds in the OLMS system are monotonically increasing with receiveDate
across all form types. Given the last rptId we've processed, we can:

  1. Find the current global max-assigned rptId via Bayesian-style
     bisection on `orgReport.do` (probing multiple form types, since a
     given rptId is assigned to exactly one form).
  2. Forward-scan the new window for ones that are LM-10 (PDF or HTML
     containing "Signature").
  3. For each electronic hit, parse `1. File Number: E-...` to recover
     the filer's srNum.

Output: srNums, one per line, on stdout. Suitable for:
    scrapy crawl filings_incremental -a sr_nums_file=sr_nums.txt

Paper LM-10s return PDFs; their srNum isn't trivially extractable
without OCR, so we emit a warning and rely on the periodic full backfill
to pick them up. In recent windows we've seen 0 paper hits.

Usage:
    python tools/discover_new_filings.py --max-known 938562
    python tools/discover_new_filings.py --max-known-from-db lm10.db
    python tools/discover_new_filings.py  # defaults to labordata.bunkum.us
"""

import argparse
import math
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import erf, sqrt

import requests
import urllib3
from parsel import Selector

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REPORT_URL = "https://olmsapps.dol.gov/query/orgReport.do?rptId={}&rptForm={}"
PROBE_FORMS = ("LM2Form", "LM10Form", "LM20Form", "LM30Form", "S1Form")
STUB_MAX = 10_000  # any text/html body larger than this indicates real content


def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "lm10-discover"
    s.verify = False
    return s


def fetch_assigned(session, rpt_id, form):
    r = session.get(REPORT_URL.format(rpt_id, form), timeout=30)
    ct = r.headers.get("Content-Type", "").split(";")[0].strip()
    if ct == "application/pdf":
        return True
    if ct == "text/html" and (b"Signature" in r.content or len(r.content) > STUB_MAX):
        return True
    return False


def is_assigned(session, rpt_id):
    with ThreadPoolExecutor(max_workers=len(PROBE_FORMS)) as ex:
        return any(ex.map(lambda f: fetch_assigned(session, rpt_id, f), PROBE_FORMS))


def fetch_lm10(session, rpt_id):
    r = session.get(REPORT_URL.format(rpt_id, "LM10Form"), timeout=30)
    ct = r.headers.get("Content-Type", "").split(";")[0].strip()
    if ct == "application/pdf":
        return "paper", r.content
    if ct == "text/html" and b"Signature" in r.content:
        return "electronic", r.content
    return None, r.content


def bayesian_bisect(session, low, prior_mean, prior_sigma, max_probes=20):
    """Find largest assigned rptId > `low` using log-normal prior on growth."""
    lo = low
    step = max(prior_mean, 100)
    probe = low + step
    while True:
        if is_assigned(session, probe):
            lo = probe
            step *= 2
            probe = low + step
            if step > 10_000_000:
                raise RuntimeError("runaway expansion")
        else:
            hi = probe
            break

    mu = math.log(prior_mean)
    sigma = math.log(1 + prior_sigma / prior_mean)

    def cdf(log_o):
        return 0.5 * (1 + erf((log_o - mu) / (sigma * sqrt(2))))

    for _ in range(max_probes):
        if hi - lo <= 1:
            break
        oa, ob = max(lo - low, 1), hi - low
        pa, pb = cdf(math.log(oa)), cdf(math.log(ob))
        target = (pa + pb) / 2
        l, r = math.log(oa), math.log(ob)
        for _ in range(40):
            m = (l + r) / 2
            (l, r) = (m, r) if cdf(m) < target else (l, m)
        mid = low + int(math.exp((l + r) / 2))
        mid = max(lo + 1, min(hi - 1, mid))
        if is_assigned(session, mid):
            lo = mid
        else:
            hi = mid
    return lo


def extract_sr_num(html_bytes):
    """Pull `1. File Number: E-XXXXX` out of an LM-10 HTML report."""
    sel = Selector(text=html_bytes.decode("utf-8", errors="replace"))
    val = sel.xpath(
        "//span[@class='i-label' and "
        "normalize-space(text())='1. File Number: E-']"
        "/following-sibling::span[@class='i-value'][1]/text()"
    ).get()
    if val:
        return int(val.strip())
    return None


def get_max_known(args):
    if args.max_known:
        return args.max_known
    if args.max_known_from_db:
        with sqlite3.connect(args.max_known_from_db) as conn:
            row = conn.execute(
                "SELECT max(rptId) FROM filing WHERE formFiled = 'LM-10'"
            ).fetchone()
            return row[0]
    sess = _session()
    q = (
        "https://labordata.bunkum.us/lm10/-/query.json?"
        "sql=select+max(rptId)+as+m+from+filing&_shape=array"
    )
    return sess.get(q, timeout=30).json()[0]["m"]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-known", type=int)
    ap.add_argument("--max-known-from-db")
    ap.add_argument("--prior-mean", type=int, default=10_000)
    ap.add_argument("--prior-sigma", type=int, default=15_000)
    ap.add_argument("--concurrency", type=int, default=6)
    args = ap.parse_args()

    sess = _session()
    max_known = get_max_known(args)
    print(f"# max known LM-10 rptId: {max_known}", file=sys.stderr)

    t0 = time.perf_counter()
    max_assigned = bayesian_bisect(sess, max_known, args.prior_mean, args.prior_sigma)
    print(f"# bisected max assigned rptId: {max_assigned} "
          f"({time.perf_counter() - t0:.1f}s)", file=sys.stderr)

    window = list(range(max_known + 1, max_assigned + 1))
    if not window:
        print(f"# no new rptIds since {max_known}", file=sys.stderr)
        return

    print(f"# forward-scanning {len(window)} candidates...", file=sys.stderr)
    t0 = time.perf_counter()
    hits = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = {ex.submit(fetch_lm10, sess, r): r for r in window}
        for done in as_completed(futures):
            rpt = futures[done]
            kind, body = done.result()
            if kind:
                hits.append((rpt, kind, body))
    hits.sort()
    print(f"# found {len(hits)} new LM-10s in "
          f"{time.perf_counter() - t0:.1f}s", file=sys.stderr)

    sr_nums = set()
    paper_unresolved = 0
    for rpt, kind, body in hits:
        if kind == "paper":
            paper_unresolved += 1
            print(f"# paper LM-10 at rpt={rpt} — srNum unresolved "
                  f"(will be caught by next full backfill)", file=sys.stderr)
            continue
        sr = extract_sr_num(body)
        if sr is None:
            print(f"# could not extract srNum from rpt={rpt}", file=sys.stderr)
            continue
        sr_nums.add(sr)

    print(f"# {len(sr_nums)} unique filers with new activity",
          file=sys.stderr)
    if paper_unresolved:
        print(f"# {paper_unresolved} paper filings deferred",
              file=sys.stderr)

    for sr in sorted(sr_nums):
        print(sr)


if __name__ == "__main__":
    main()
