from scrapy import Spider
from scrapy.http import FormRequest


class Organizations(Spider):
    name = "organizations"

    # Match the schema of the existing `organization` table. The
    # GetAdditionalEmpsLM10Servlet response carries the full LM-10
    # detail shape (28 fields), but the table only stores six.
    custom_settings = {
        "FEED_EXPORT_FIELDS": [
            "promiseDate",
            "oID",
            "rptId",
            "empLabOrg",
            "city",
            "state",
        ],
    }

    async def start(self):
        yield FormRequest(
            "https://olmsapps.dol.gov/olpdr/GetLM10FilerListServlet",
            formdata={"clearCache": "F", "page": "1"},
            cb_kwargs={"page": 1},
            callback=self.parse,
        )

    def parse(self, response, page):
        """
        @url https://olmsapps.dol.gov/olpdr/GetLM2021FilerListServlet
        @filers_form
        @cb_kwargs {"page": 0}
        @returns requests 501 501
        """

        filers = response.json()["filerList"]
        for filer in filers:
            yield self._detail_request(filer["srNum"])
        if len(filers) == 500:
            page += 1
            yield FormRequest(
                "https://olmsapps.dol.gov/olpdr/GetLM10FilerListServlet",
                formdata={"clearCache": "F", "page": str(page)},
                cb_kwargs={"page": page},
                callback=self.parse,
            )

    def _detail_request(self, sr_num):
        return FormRequest(
            "https://olmsapps.dol.gov/olpdr/GetLM10FilerDetailServlet",
            formdata={"srNum": "C-" + str(sr_num)},
            callback=self.parse_filings,
        )

    def _iter_filings(self, response):
        return response.json()["detail"]

    def parse_filings(self, response):
        """
        @url https://olmsapps.dol.gov/olpdr/GetLM2021FilerDetailServlet
        @filings_form
        @returns request 1
        """

        for filing in self._iter_filings(response):
            yield FormRequest(
                "https://olmsapps.dol.gov/olpdr/GetAdditionalEmpsLM10Servlet",
                formdata={"rptId": str(filing["rptId"])},
                callback=self.parse_organization,
            )

    def parse_organization(self, response):
        """
        @url https://olmsapps.dol.gov/olpdr/GetAdditionalEmpsServlet
        @organization_form
        @returns items 1
        """
        for organization in response.json()["detail"]:
            yield organization


class IncrementalOrganizations(Organizations):
    """Fetch additional-employer rows for new filings only.

    Inputs match `IncrementalFilings`: list of srNums plus optional
    max_known_rpt_id to skip already-known rptIds. For each new filing,
    one POST to GetAdditionalEmpsLM10Servlet."""

    name = "organizations_incremental"

    def __init__(
        self, sr_nums=None, sr_nums_file=None, max_known_rpt_id=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        nums = []
        if sr_nums:
            nums.extend(sr_nums.split(","))
        if sr_nums_file:
            with open(sr_nums_file) as f:
                nums.extend(f.read().split())
        if not nums:
            raise ValueError(
                "pass either -a sr_nums=42,556,1213 or -a sr_nums_file=/path/to/file"
            )
        self.sr_nums = sorted({int(n) for n in nums if n.strip()})
        self.max_known_rpt_id = int(max_known_rpt_id) if max_known_rpt_id else None

    async def start(self):
        for sr in self.sr_nums:
            yield self._detail_request(sr)

    def _iter_filings(self, response):
        for filing in response.json()["detail"]:
            if (
                self.max_known_rpt_id is not None
                and filing["rptId"] <= self.max_known_rpt_id
            ):
                continue
            yield filing
