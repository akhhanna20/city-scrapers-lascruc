import re
from datetime import timedelta

import scrapy
from city_scrapers_core.constants import BOARD, COMMITTEE, NOT_CLASSIFIED
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider
from dateutil.parser import parse as dateparse
from scrapy_playwright.page import PageMethod


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
            "args": ["--no-sandbox"],
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
        # date_str (YYYY-MM-DD) -> list of {title, video_href}
        self.video_links = {}

    # Title normalization — maps known variations
    TITLE_MAP = {
        "work session": "board of education",
        "regular session": "board of education",
        "retreat": "board of education",
        "retreat (extended work session)": "board of education",
        "special board meeting": "board of education",
        "special meeting": "board of education",
        "extended work session": "board of education",
        "finance subcommittee meeting": "finance subcommittee",
        "finance committee": "finance subcommittee",
        "budget town hall ii meeting": "budget town hall meeting",
        "budget town hall ii": "budget town hall meeting",
    }

    LOCATION_MAP = {
        "Trujillo": {
            "name": "Dr. Karen M. Trujillo Administration Complex, Board Room",
            "address": "505 S. Main St., Suite 249, Las Cruces, NM 88001",
        },
        "Administration Complex": {
            "name": "Dr. Karen M. Trujillo Administration Complex",
            "address": "505 S. Main St., Suite 249, Las Cruces, NM 88001",
        },
        "Cesar Chavez": {
            "name": "Cesar Chavez Elementary School, Cafeteria",
            "address": "5250 Holman Road, Las Cruces, NM 88012",
        },
        "Organ Mountain": {
            "name": "Organ Mountain High School Library",
            "address": "5700 Mesa Grande Drive, Las Cruces, NM 88012",
        },
        "Ana Community College": {
            "name": "Dona Ana Community College",
            "address": "2800 N. Sonoma Ranch Blvd, Las Cruces, NM 88011",
        },
    }

    def _normalize_title(self, title):
        """Lowercase, strip, and resolve known variations."""
        t = " ".join(title.lower().split())
        # Remove trailing date patterns
        t = re.sub(
            r"\s+(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|"  # 5-15-2025 or 5/15/2025
            r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|"  # 2025-05-15
            r"(?:jan|feb|mar|apr|may|jun|"
            r"jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4})$",  # May 15, 2025
            "",
            t,
        ).strip()
        return self.TITLE_MAP.get(t, t)

    def _build_href(self, href):
        """Normalize protocol-relative URLs to https."""
        return ("https:" + href) if href.startswith("//") else href

    def start_requests(self):
        """First fetch attachments from the secondary source (plain HTTP), then scrape lcps.net with Playwright."""  # noqa
        yield scrapy.Request(
            url=self.secondary_source_url,
            callback=self._parse_secondary_source,
            dont_filter=True,
        )

    def _parse_secondary_source(self, response):
        """
        Scrapes the secondary URL video links, then kicks off the main
        Playwright request to lcps.net.
        The main lcps.net page renders its content via JavaScript, so a real
        Playwright is used instead of a plain HTTP request.
        Playwright expands all collapsed accordion sections by clicking their
        headers before handing the fully rendered HTML back to Scrapy
        """
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

            # Video link from onclick JS attribute
            video_href = ""
            onclick = row.css("td.listItem[headers^='VideoLink'] a::attr(onclick)").get(
                ""
            )
            m = re.search(r"window\.open\('([^']+)'", onclick)
            if m:
                video_href = self._build_href(m.group(1))

            self.video_links.setdefault(date_key, []).append(
                {
                    "norm_title": self._normalize_title(title_raw),
                    "video_href": video_href,
                }
            )

        # Playwright request
        yield scrapy.Request(
            url=self.source_url,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # Waits for a table to appear
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

        for panel in response.css(".pb-accordion-panel"):
            panel_title = panel.css(".panel-title::text").get("").strip()
            if not re.search(r"\d{4}-\d{4}", panel_title):
                continue
            for table in panel.css("table"):
                yield from self._parse_main_table(table)

    def _parse_main_table(self, table_sel):
        for row in table_sel.css("tr")[1:]:
            cells = row.css("td")
            if not cells or not (
                cells[0].css("p::text").get("").strip()
                or cells[0].css("p span::text").get("").strip()
            ):
                continue

            date_text = (
                cells[0].css("p::text").get() or cells[0].css("p span::text").get("")
            ).strip()
            time_text = (
                cells[1].css("p::text").get() or cells[1].css("p span::text").get("")
            ).strip()
            # Detect cancellation in the time column
            is_cancelled = (
                "cancelled" in time_text.lower() or "canceled" in time_text.lower()
            )
            if is_cancelled:
                time_text = ""

            location_text = (
                cells[2].css("p span::text").get() or cells[2].css("p::text").get("")
            ).strip()
            meeting_type = cells[3].css("p::text, p span::text").get("").strip()

            if not date_text or not meeting_type:
                continue

            start = self._parse_dt(f"{date_text} {time_text}") or self._parse_dt(
                date_text
            )
            if not start:
                continue

            links = self._parse_links(cells)
            links = self._attach_video(links, start, meeting_type)

            parsed_location = self._parse_location(location_text)
            is_virtual = "virtual" in location_text.lower()
            is_unknown_address = not location_text or parsed_location["address"] == ""

            time_notes = ""
            if is_virtual or is_unknown_address:
                time_notes = "Please refer to the meeting attachments for the meeting time and location."  # noqa

            status_text = f"{meeting_type} CANCELLED" if is_cancelled else meeting_type

            meeting = Meeting(
                title=self._parse_title(meeting_type),
                description="",
                classification=self._parse_classification(meeting_type),
                start=start,
                end=None,
                all_day=False,
                time_notes=time_notes,
                location=parsed_location,
                links=links,
                source=self.source_url,
            )

            meeting["status"] = self._get_status(meeting, status_text)
            meeting["id"] = self._get_id(meeting)

            yield meeting

    def _attach_video(self, links, start, meeting_type):
        norm = self._normalize_title(meeting_type)
        for delta in range(-1, 2):
            key = (start + timedelta(days=delta)).strftime("%Y-%m-%d")
            match = next(
                (v for v in self.video_links.get(key, []) if v["norm_title"] == norm),
                None,
            )
            if match:
                if match["video_href"]:
                    links.append({"href": match["video_href"], "title": "Video"})
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
            # Normalize a.m./p.m. → AM/PM
            clean = (
                text.replace("a.m.", "AM")
                .replace("p.m.", "PM")
                .replace("A.M.", "AM")
                .replace("P.M.", "PM")
            )

            return dateparse(clean, fuzzy=True)

        except (ValueError, OverflowError):
            self.logger.warning(f"Could not parse date/time from text: {text!r:.200}")
            return None

    def _parse_location(self, location_text):
        if not location_text:
            return {"name": "", "address": ""}
        if "Virtual" in location_text:
            return {"name": "Virtual Meeting", "address": ""}
        for keyword, location in self.LOCATION_MAP.items():
            if keyword in location_text:
                return location
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
                title = a.css("::text").get("").strip() or default_title
                if href:
                    links.append({"href": href, "title": title})
        return links if links else [{"href": "", "title": ""}]
