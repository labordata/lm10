"""Form-encoded POST helper.

scrapy.http.FormRequest is deprecated in 2.16 in favor of
the form2request library, which is designed for parsing scraped
`<form>` elements. None of our spiders do that — they just POST
formdata to known endpoints — so we wrap scrapy.Request directly.
"""

from urllib.parse import urlencode

from scrapy import Request


def form_request(url, formdata=None, **kwargs):
    headers = kwargs.pop("headers", {}) or {}
    headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    return Request(
        url=url,
        method="POST",
        body=urlencode(formdata or {}),
        headers=headers,
        **kwargs,
    )
