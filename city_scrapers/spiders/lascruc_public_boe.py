import re
from datetime import timedelta

import scrapy
from city_scrapers_core.constants import BOARD, COMMITTEE, NOT_CLASSIFIED
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider
from dateutil.parser import parse as dateparse
from scrapy_playwright.page import PageMethod

# Title normalization — maps Granicus titles → canonical titles
TITLE_MAP = {
    "work session": "board of education",
    "regular session": "board of education",
    "finance subcommittee meeting": "finance subcommittee",
    "finance committee": "finance subcommittee",
    "budget town hall ii meeting": "budget town hall meeting",
    "budget town hall ii": "budget town hall meeting",
}


def _normalize_title(title):
    """Lowercase, strip, and resolve known variations."""
    t = " ".join(title.lower().split())
    return TITLE_MAP.get(t, t)


class LascrucPublicBoeSpider(CityScrapersSpider):
    name = "lascruc_public_boe"
    agency = "Las Cruces Public Schools Board of Education"
    timezone = "America/Denver"

    source_url = "https://www.lcps.net/page/board-documents-new"
    secondary_source_url = "https://lcpsnm.granicus.com/ViewPublisher.php?view_id=1"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-gpu",
                "--blink-settings=imagesEnabled=false",
            ],
        },
        "DOWNLOAD_HANDLERS": {
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # date_str (YYYY-MM-DD) -> list of {title, agenda_href, video_href}
        self.additional_links = {}

    def start_requests(self):
        """First fetch attachments from the secondary source (plain HTTP), then scrape lcps.net with Playwright."""  # noqa
        yield scrapy.Request(
            url=self.secondary_source_url,
            callback=self._parse_secondary_source,
            dont_filter=True,
        )

    def _parse_secondary_source(self, response):
        for row in response.css("tr.listingRow"):
            title_raw = row.css("td.listItem[headers^='Name']::text").get("").strip()
            date_text = (
                " ".join(row.css("td.listItem[headers^='Date'] ::text").getall())
                .replace("\xa0", " ")
                .strip()
            )

            if not title_raw or not date_text:
                continue

            start = self._parse_dt(date_text)
            if not start:
                continue

            date_key = start.strftime("%Y-%m-%d")

            #  Agenda link
            agenda_href = ""
            agenda_a = row.css("td.listItem a[href*='AgendaViewer']")
            if agenda_a:
                href = agenda_a.attrib["href"]
            agenda_href = ("https:" + href) if href.startswith("//") else href

            # Video link from onclick JS attribute
            video_href = ""
            onclick = row.css("td.listItem[headers^='VideoLink'] a::attr(onclick)").get(
                ""
            )
            m = re.search(r"window\.open\('([^']+)'", onclick)
            if m:
                href = m.group(1)
            video_href = ("https:" + href) if href.startswith("//") else href

            self.additional_links.setdefault(date_key, []).append(
                {
                    "norm_title": _normalize_title(title_raw),
                    "agenda_href": agenda_href,
                    "video_href": video_href,
                }
            )

        # Playwright request
        yield scrapy.Request(
            url=self.source_url,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "table"),
                    # Click on all accordion headers
                    PageMethod(
                        "evaluate",
                        "document.querySelectorAll('.panel-title').forEach(e => e.click())",  # noqa
                    ),
                    PageMethod("wait_for_timeout", 1000),
                ],
            },
            callback=self.parse,
        )

    def parse(self, response):
        """Parse both the current-year table and all archive accordion tables."""
        yield from self._parse_main_table(response.css("div.pb-table table"))

        for panel in response.css(".panel, .accordion-item, [data-v-96d80c27]"):
            panel_title = (
                panel.css(".panel-title::text").get("")
                or panel.css(".accordion-header::text").get("")
            ).strip()
            if not re.search(r"\d{4}-\d{4}", panel_title):
                continue
            for table in panel.css("table"):
                yield from self._parse_main_table(table)

    def _parse_main_table(self, table_sel):
        for row in table_sel.css("tr")[1:]:
            cells = row.css("td")
            if not cells or not cells[0].css("p::text").get("").strip():
                continue

            date_text = cells[0].css("p::text").get(default="").strip()
            time_text = cells[1].css("p::text").get(default="").strip()
            location_text = (
                cells[2].css("p span::text").get()
                or cells[2].css("p::text").get(default="")
            ).strip()
            meeting_type = cells[3].css("p::text, p span::text").get(default="").strip()

            if not date_text or not meeting_type:
                continue

            time_clean = time_text.replace("p.m.", "PM").replace("a.m.", "AM")
            start = self._parse_dt(f"{date_text} {time_clean}") or self._parse_dt(
                date_text
            )
            if not start:
                continue

            links = self._parse_links(cells)
            links = self._attach_video(links, start, meeting_type)

            meeting = Meeting(
                title=self._parse_title(meeting_type),
                description="",
                classification=self._parse_classification(meeting_type),
                start=start,
                end=None,
                all_day=False,
                time_notes="",
                location=self._parse_location(location_text),
                links=links,
                source=self.source_url,
            )

            meeting["status"] = self._get_status(meeting, meeting_type)
            meeting["id"] = self._get_id(meeting)

            yield meeting

    def _attach_video(self, links, start, meeting_type):
        norm = _normalize_title(meeting_type)
        for delta in range(-1, 2):
            key = (start + timedelta(days=delta)).strftime("%Y-%m-%d")
            match = next(
                (
                    v
                    for v in self.additional_links.get(key, [])
                    if v["norm_title"] == norm
                ),
                None,
            )
            if match:
                if match["video_href"]:
                    links.append({"href": match["video_href"], "title": "Video"})
                if match["agenda_href"]:
                    links.append(
                        {"href": match["agenda_href"], "title": "Agenda (Granicus)"}
                    )
                break
        return links

    def _parse_title(self, meeting_type):
        return " ".join(meeting_type.split()).strip()

    def _parse_classification(self, meeting_type):
        title_lower = meeting_type.lower()
        if "committee" in title_lower or "subcommittee" in title_lower:
            return COMMITTEE
        if (
            "session" in title_lower
            or "work" in title_lower
            or "board" in title_lower
            or "town hall" in title_lower
            or "retreat" in title_lower
        ):
            return BOARD
        return NOT_CLASSIFIED

    def _parse_dt(self, text):
        try:
            return dateparse(
                text, fuzzy=True, tzinfos={"PM": None, "AM": None, "M": None}
            )
        except (ValueError, OverflowError):
            return None

    def _parse_location(self, location_text):
        if not location_text:
            return {"name": "", "address": ""}
        if "Virtual" in location_text:
            return {"name": "Virtual Meeting", "address": ""}
        if "Trujillo" in location_text or "Administration Complex" in location_text:
            parts = location_text.split(",")
            room = parts[-1].strip() if len(parts) > 1 else ""
            return {
                "name": f"Dr. Karen M. Trujillo Administration Complex{', ' + room if room else ''}",  # noqa
                "address": "505 S. Main St., Suite 249, Las Cruces, NM 88001",
            }
        return {"name": location_text, "address": ""}

    def _parse_links(self, cells):
        links = []
        link_columns = {
            4: "Legal Notice",
            5: "Agenda",
            6: "Packet",
            7: "Minutes",
        }
        for idx, default_title in link_columns.items():
            if idx >= len(cells):
                break
            for a in cells[idx].css("a"):
                href = a.attrib.get("href", "").strip()
                title = a.css("::text").get(default="").strip() or default_title
                if href:
                    links.append({"href": href, "title": title})
        return links if links else [{"href": "", "title": ""}]
