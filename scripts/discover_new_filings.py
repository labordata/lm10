"""Discover filers (srNums) with new LM-10 activity since lm10.db's
max LM-10 rptId.

Usage: python scripts/discover_new_filings.py lm20.db
"""

from olms.discover import DiscoveryConfig, main

CONFIG = DiscoveryConfig(
    watermark_sql="SELECT max(rptId) FROM filing",
    scan_forms=("LM10Form",),
    sr_num_labels=("1. File Number: E-",),
    description=__doc__,
)

if __name__ == "__main__":
    main(CONFIG)
    