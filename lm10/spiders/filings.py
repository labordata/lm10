import re
from email.message import Message

from scrapy import Spider
from scrapy.http import FormRequest, Request


class LM20(Spider):
    name = "filings"

    custom_settings = {
        "ITEM_PIPELINES": {
            "lm10.pipelines.TimestampToDatetime": 1,
            "lm10.pipelines.HeaderMimetypePipeline": 3,
        }
    }

    def start_requests(self):
        return [
            FormRequest(
                "https://olmsapps.dol.gov/olpdr/GetLM10FilerListServlet",
                formdata={"clearCache": "F", "page": "1"},
                cb_kwargs={"page": 1},
                callback=self.parse,
            )
        ]

    def parse(self, response, page):
        """
        @url https://olmsapps.dol.gov/olpdr/GetLM10FilerListServlet
        @filers_form
        @cb_kwargs {"page": 0}
        @returns requests 501 501
        """
        filers = response.json()["filerList"]
        for filer in filers:
            yield FormRequest(
                "https://olmsapps.dol.gov/olpdr/GetLM10FilerDetailServlet",
                formdata={"srNum": "C-" + str(filer["srNum"])},
                callback=self.parse_filings,
            )
        if len(filers) == 500:
            page += 1
            yield FormRequest(
                "https://olmsapps.dol.gov/olpdr/GetLM10FilerListServlet",
                formdata={"clearCache": "F", "page": str(page)},
                cb_kwargs={"page": page},
                callback=self.parse,
            )

    def parse_filings(self, response):
        """
        @url https://olmsapps.dol.gov/olpdr/GetLM10FilerDetailServlet
        @filings_form
        @returns items 2
        @scrapes amended
        """
        for filing in response.json()["detail"]:
            del filing["attachmentId"]
            del filing["fileName"]
            del filing["fileDesc"]

            filing["detailed_form_data"] = None
            filing["file_urls"] = []
            filing["file_headers"] = {}

            report_url = "https://olmsapps.dol.gov/query/orgReport.do?rptId={rptId}&rptForm={formLink}".format(
                **filing
            )

            filing["filing_url"] = report_url

            # These three conditions seem sufficient to identify that
            # a filing was submitted electronically and if we
            # request the filing form, we'll get back a web page
            # (though we might have to ask more than once)
            electronic_submission = all(
                (
                    filing["paperOrElect"] == "E",
                    filing["receiveDate"],
                    filing["rptId"] > 183135,
                )
            )

            if electronic_submission:
                yield Request(
                    report_url,
                    cb_kwargs={"item": filing, "report": LM10Report},
                    callback=self.parse_html_report,
                )
            else:
                yield Request(
                    report_url,
                    method="HEAD",
                    cb_kwargs={"item": filing},
                    callback=self.report_header,
                )

    def report_header(self, response, item):

        item["file_urls"] = [response.request.url]
        item["file_headers"] = {
            response.request.url: dict(response.headers.to_unicode_dict())
        }

        yield item

    def parse_html_report(self, response, item, report, attempts=0):

        # baffling, sometimes when you request some resources
        # it returns html and sometime it returns a pdf

        m = Message()
        m["content-type"] = response.headers.get("Content-Type").decode()

        content_type, _ = m.get_params()[0]

        keep_trying = True

        if content_type == "text/html" and b"Signature" in response.body:

            form_data = report.parse(response)

            if str(item["srNum"]) == form_data[
                "file_number"
            ] or attempts > self.settings.getint("MISMATCHED_FILER_RETRY", 2):
                item["detailed_form_data"] = form_data
                yield item
                keep_trying = False
            else:
                attempts += 1

        if keep_trying:
            yield Request(
                response.request.url,
                cb_kwargs={"item": item, "report": report, "attempts": attempts},
                callback=self.parse_html_report,
                dont_filter=True,
            )


class LM10Report:
    @classmethod
    def parse(cls, response):

        form_dict = {
            "file_number": cls._get_i_value(response, "1. File Number: E-"),
            "period_begin": cls._get_i_value(response, "From:"),
            "period_through": cls._get_i_value(response, "Through:"),
            "reporting_employer": [cls._section_three(response)],
            "principal_officer": [cls._section_four(response)],
            "other_address": [cls._section_five(response)],
            "where_records": cls._where_records(response),
            "type_of_organization": cls._type_of_org(response),
            "signatures": cls._signatures(response),
            "reportable_activity": cls._reportable_activity(response),
            "activity_details": cls._activity_details(response),
        }

        form_dict = normalize_space(form_dict)

        return form_dict

    @classmethod
    def _activity_details(cls, response):
        activities = []

        activity_tables = response.xpath(
            "//div[@class='myTable' and descendant::span[@class='i-label' and text()='9.a.']]"
        )

        for table in activity_tables:
            activity = {
                "activity_code": table.xpath(
                    "./preceding-sibling::div[@class='myTable'][1]"
                    "//div[@class='i-sectionNumberTable' and descendant::span[@class='i-label' and text()=' Check Item Number(from Page 2) to which this Part B applies']]"
                    "//span[text()='X']/parent::div/text()"
                ).get(),
                "activity_type": table.xpath(
                    ".//span[@class='i-label' and text()='9.a.']"
                    "/following-sibling::span[@class='i-value']"
                    "/span[text()='X']/following::text()[1]"
                ).get(),
                "counterparty_position": cls._get_i_value(
                    table,
                    "9.c. Position In labor organization or with employer (if an independent labor consultant, so state).",
                ),
                "counterparty_contact": [
                    cls._parse_section(
                        table,
                        section_label="9.b. Name and address of person with whom or through whom a separate agreement was made or to whom payments were made.",
                        field_labels=(
                            "Name:",
                            "P.O. Box., Bldg., Room No., if any:",
                            "Street:",
                            "City:",
                            "State:",
                            "ZIP Code + 4:",
                        ),
                    )
                ],
                "counterparty_organization": [
                    cls._parse_section(
                        table,
                        section_label="9.d. Name and address of firm or labor organization with whom employed or affiliated.",
                        field_labels=(
                            "Organization:",
                            "P.O. Box., Bldg., Room No., if any:",
                            "Street:",
                            "City:",
                            "State:",
                            "ZIP Code + 4:",
                        ),
                    )
                ],
                "date_of_agreement": cls._section(table, "10.a.")
                .xpath(".//span[@class='i-value' and normalize-space(text())]/text()")
                .get(),
                "form_agreement": table.xpath(
                    ".//div[@class='i-sectionNumberTable' and descendant::span[@class='i-label' and normalize-space(text())='10.b.']]"
                    "/parent::div"
                    "//span[text()='X']"
                    "/following::text()"
                ).get(),
                "explanation": cls._get_i_value(
                    table,
                    "Explain fully the circumstances of all payments, including the terms of any oral agreement or understanding pursuant to which they were made :",
                ),
                "12b_exists": (
                    table.xpath(".//span[@class='i-label' and text()='12b.']").get()
                    != None
                ),
                "federal_work": None,
                "uei": None,
                "no_uei_checkbox": None,
                "agencies": None,
                "unlisted_agencies": None,
            }

            if activity["12b_exists"]:
                # Assign values to those None defaulted fields
                activity = cls._get_12b(activity, table)

            expenditure_table = table.xpath(
                ".//table[@class='addTable' and descendant::span[@class='i-label' and normalize-space(text())='11.a. Date of each payment or expenditure (mm/dd/yyyy).']]"
            )

            expenditures = []
            for row in expenditure_table.xpath(
                ".//tr[descendant::span[@class='i-value']]"
            ):
                expenditures.append(
                    dict(
                        zip(
                            ("date", "amount", "kind"),
                            row.xpath(".//span[@class='i-value']/text()").getall(),
                        )
                    )
                )

            activity["expenditures"] = expenditures

            activities.append(activity)
        return activities

    @classmethod
    def _reportable_activity(cls, response):

        section = response.xpath(
            "//div[@class='i-sectionNumberTable' and descendant::div[@class='i-label' and text()=' Type of Reportable Activity Engaged In By Employer']]"
            "/parent::div"
            "/div[@class='activityTable']"
        )

        questions = section.xpath(
            "./div[@class='row' and descendant::span[@class='i-value']]"
        )

        results = []

        for question in questions:
            code = question.xpath(
                "./div[@class='col-xs-1  notop']/span[@class='i-value']/text()"
            ).get()

            query = question.xpath(
                ".//div[@class='col-xs-16  notop']/span[@class='i-value']/text()"
            ).get()

            answer = question.xpath(
                ".//div[@class='col-xs-5  notop']"
                "//span[text()='X']"
                "/preceding-sibling::span[@class='i-value'][1]"
                "/text()"
            ).get()

            n_responses = question.xpath(
                ".//div[@class='col-xs-2  notop']"
                "//span[@class='i-xcheckbox']"
                "/text()"
            ).get()

            results.append(
                {
                    "question": query,
                    "answer": answer,
                    "n_responses": n_responses,
                    "code": code,
                }
            )

        return results

    @classmethod
    def _where_records(cls, response):

        label_text = """Indicate by checking the appropriate box or boxes where records
														necessary to verify this report will be available for examination."""

        section = response.xpath(
            f"//div[@class='i-sectionNumberTable' and descendant::span[@class='i-label' and text()='{label_text}']]/parent::div"
        )

        which_address = {}

        for option, number in (
            ("records_hosted_with_reporting_employer", "3"),
            ("records_hosted_with_principal_officer", "4"),
            ("records_hosted_at_other_address", "5"),
        ):

            checkbox = section.xpath(
                f".//div[@class='i-sectionbody' and contains(., 'Address in Item {number}')]"
                "//span[@class='i-xcheckbox']"
                "/text()"
            )
            which_address[option] = checkbox.get()

        return which_address

    @classmethod
    def _type_of_org(cls, response):

        org_type = {
            "Corporation": "Corporation",
            "Partnership": "Partnership",
            "Individual": "Individual",
            "Other": "Other",
        }

        section = response.xpath(
            ".//div[@class='i-sectionNumberTable' and descendant::span[@class='i-label' and text()='7. Type of organization.']]"
        )

        checkboxes = section.xpath(
            ".//span[@class='i-xcheckbox' or @class='i-nocheckbox']"
        )
        checked_value = None

        for box in checkboxes:
            value = box.xpath("./text()").get(default="").strip()
            label = org_type[
                normalize_space(box.xpath("./following-sibling::text()[1]").get())
            ]

            if value == "X":
                assert checked_value is None
                checked_value = label

        return checked_value

    @classmethod
    def _signatures(cls, response):

        result = {13: {}, 14: {}}
        for signature_number in result:
            section = cls._signature_section(response, signature_number)
            for field in ("SIGNED:", "Title:", "On Date:", "Telephone Number:"):
                result[signature_number][clean_field(field)] = cls._get_i_value(
                    section, field
                )

        return result

    @classmethod
    def _signature_section(cls, response, signature_number):

        result = response.xpath(
            f"//div[@class='myTable' and descendant::span[@class='i-label' and text()='{signature_number}.']]"
        )[1]

        return result

    @classmethod
    def _section_three(cls, response):
        return cls._parse_section(
            response,
            section_label="3. Name and address of Reporting Employer (inc. trade name, if any).",
            field_labels=(
                "Employer:",
                "Trade Name:",
                "Attention To:",
                "Title:",
                "P.O. Box., Bldg., Room No., if any:",
                "Street:",
                "City:",
                "State:",
                "ZIP Code + 4:",
            ),
        )

    @classmethod
    def _section_four(cls, response):
        return cls._parse_section(
            response,
            section_label="4. Name and address of President or corresponding principal officer, if different from address in Item 3.",
            field_labels=(
                "Name:",
                "P.O. Box., Bldg., Room No., if any:",
                "Street:",
                "City:",
                "State:",
                "ZIP Code + 4:",
            ),
        )

    @classmethod
    def _section_five(cls, response):
        return cls._parse_section(
            response,
            section_label="Any other address where records necessary to verify this report will be available for examination.",
            field_labels=(
                "Name:",
                "Title:",
                "Organization:",
                "P.O. Box., Bldg., Room No., if any:",
                "Street:",
                "City:",
                "State:",
                "ZIP Code + 4:",
            ),
        )

    @classmethod
    def _parse_section(cls, response, section_label, field_labels):
        section = cls._section(response, section_label)

        section_dict = {}
        for field in field_labels:
            section_dict[clean_field(field)] = cls._get_i_value(section, field)
        return section_dict

    @classmethod
    def _section(cls, response, label_text):

        xpath = (
            f".//div[@class='i-sectionNumberTable' and descendant::span[@class='i-label' and normalize-space(text())='{label_text}']]"
            "/following-sibling::div[@class='i-sectionbody']"
        )
        return response.xpath(xpath)

    @classmethod
    def _get_i_value(cls, tree, label_text):

        i_value_xpath = (
            f".//span[@class='i-label' and normalize-space(text())='{label_text}']"
            "/following-sibling::span[@class='i-value'][1]"
            "/text()"
        )
        result = tree.xpath(i_value_xpath)

        if not result:
            i_checkbox_xpath = (
                f".//span[@class='i-label' and normalize-space(text())='{label_text}']"
                "/following-sibling::span[@class='i-xcheckbox'][1]"
                "/text()"
            )
            result = tree.xpath(i_checkbox_xpath)

        if not result:
            nested_i_value_xpath = (
                f".//span[@class='i-label' and normalize-space(text())='{label_text}']"
                "/following-sibling::span[1]"
                "/span[@class='i-value']"
                "/text()"
            )
            result = tree.xpath(nested_i_value_xpath)

        if not result:
            following_text_xpath = (
                f".//span[@class='i-label' and normalize-space(text())='{label_text}']"
                "/following-sibling::text()[1]"
            )
            result = tree.xpath(following_text_xpath)

        if not result:
            nonnormal_i_value_xpath = (
                f".//span[@class='i-label' and text()='{label_text}']"
                "/following-sibling::span[@class='i-value'][1]"
                "/text()"
            )
            result = tree.xpath(nonnormal_i_value_xpath)

        return result.get(default="")

    @classmethod
    def _get_12b(cls, activity, table):
        result = activity

        result["federal_work"] = normalize_space(
            table.xpath(
                ".//span[@class='i-label' and text()='12b.']"
                "/parent::div/parent::div"
                "/following-sibling::div"
                "//div[@class='i-value']"
                "//span[@class='i-xcheckbox']"
                "/following-sibling::text()"
            ).get()
        )

        result["uei"] = normalize_space(
            table.xpath(
                ".//div[@class='col-xs-10' and text()[contains(.,'Unique Entity Identifier (UEI):')]]/text()"
            ).get()
        )

        if table.xpath(
            ".//div[@class='col-xs-3' and text()[contains(.,'No UEI')]]//span[@class='i-xcheckbox']"
        ).get():
            result["no_uei_checkbox"] = "Checked"
        else:
            result["no_uei_checkbox"] = "Not checked"

        # When federal_work is "Yes" a new table may appear with different categories of agencies
        if table.xpath(
            ".//span[@class='i-label' and text()='12b.']/following::tbody"
        ).get():
            agencies = ""
            unlisted_agencies = ""
            for row in table.xpath(
                ".//span[@class='i-label' and text()='12b.']/following::tbody/child::tr"
            ):
                if row.xpath(".//td[1]/text()"):
                    agencies += row.xpath(".//td[1]/text()").get() + ", "
                if row.xpath(".//td[2]/text()"):
                    unlisted_agencies += row.xpath(".//td[2]/text()").get() + ", "

            if agencies == "":
                result["agencies"] = "None"
            else:
                result["agencies"] = agencies.strip(", ")

            if unlisted_agencies == "":
                result["unlisted_agencies"] = "None"
            else:
                result["unlisted_agencies"] = unlisted_agencies.strip(", ")

        return result


def clean_field(string):

    return (
        string.replace("\xa0", "")
        .strip(" :")
        .replace(".", "")
        .replace(" ", "_")
        .lower()
    )


def normalize_space(input_value):
    if isinstance(input_value, str):
        return re.sub(r"\s+", " ", input_value).strip()
    elif isinstance(input_value, list):
        return [normalize_space(item) for item in input_value]
    elif isinstance(input_value, dict):
        return {key: normalize_space(value) for key, value in input_value.items()}
    else:
        return input_value
