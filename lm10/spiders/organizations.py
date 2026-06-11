from scrapy import Spider

from lm10._http import form_request
from lm10.spiders.incremental import SrNumSpiderMixin


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
        yield form_request(
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
            yield form_request(
                "https://olmsapps.dol.gov/olpdr/GetLM10FilerListServlet",
                formdata={"clearCache": "F", "page": str(page)},
                cb_kwargs={"page": page},
                callback=self.parse,
            )

    def _detail_request(self, sr_num):
        return form_request(
            "https://olmsapps.dol.gov/olpdr/GetLM10FilerDetailServlet",
            formdata={"srNum": "C-" + str(sr_num)},
            callback=self.parse_filings,
        )

    def parse_filings(self, response):
        """
        @url https://olmsapps.dol.gov/olpdr/GetLM10FilerDetailServlet
        @filings_form
        @returns request 1
        """

        for filing in response.json()["detail"]:
            yield form_request(
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


class IncrementalOrganizations(SrNumSpiderMixin, Organizations):
    """Fetch additional-employer rows for a specific list of filers.

    Inputs match `IncrementalFilings`. For each of the filer's filings,
    one POST to GetAdditionalEmpsLM10Servlet."""

    name = "organizations_incremental"
