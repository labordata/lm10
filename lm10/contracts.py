"""Contracts for the @filers_form/@filings_form/@organization_form tags
used in spider docstrings. Without these registrations, `scrapy check`
KeyErrors on every tagged method."""

from urllib.parse import urlencode

from scrapy.contracts import Contract


class _FormContract(Contract):
    formdata = {}

    def adjust_request_args(self, args):
        args["method"] = "POST"
        args["body"] = urlencode(self.formdata)
        headers = args.setdefault("headers", {})
        headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
        return args


class FilersFormContract(_FormContract):
    name = "filers_form"
    formdata = {"clearCache": "F", "page": "1"}


class FilingsFormContract(_FormContract):
    name = "filings_form"
    formdata = {"srNum": "C-3021"}


class OrganizationFormContract(_FormContract):
    name = "organization_form"
    formdata = {"rptId": "509611"}
