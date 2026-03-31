from datetime import datetime
from os.path import dirname, join

import pytest
from city_scrapers_core.constants import BOARD, COMMITTEE
from city_scrapers_core.utils import file_response
from freezegun import freeze_time

from city_scrapers.spiders.lascruc_public_boe import LascrucPublicBoeSpider

livestream_url_response = file_response(
    join(dirname(__file__), "files", "lascruc_public_boe_agenda_livestream.html"),
    url="https://www.lcps.net/page/board-documents-new",
)

main_url_response = file_response(
    join(dirname(__file__), "files", "lascruc_public_boe.html"),
    url="https://www.lcps.net/page/board-documents-new",
)


spider = LascrucPublicBoeSpider()

freezer = freeze_time("2026-03-25")
freezer.start()

# First populate secondary source
list(spider._parse_secondary_source(livestream_url_response))
# Then parse the main table
parsed_items = [item for item in spider.parse(main_url_response)]

freezer.stop()


def test_count():
    assert len(parsed_items) == 838


def test_title():
    assert parsed_items[0]["title"] == "Regular Session"


def test_description():
    assert parsed_items[0]["description"] == ""


def test_start():
    assert parsed_items[0]["start"] == datetime(2026, 3, 24, 18, 0)


def test_end():
    assert parsed_items[0]["end"] is None


def test_time_notes():
    assert parsed_items[0]["time_notes"] == ""


def test_classification_board():
    assert parsed_items[0]["classification"] == BOARD


def test_classification_committee():
    # find a committee row
    committee_item = next(i for i in parsed_items if "committee" in i["title"].lower())
    assert committee_item["classification"] == COMMITTEE


def test_location():
    assert parsed_items[0]["location"] == {
        "name": "Dr. Karen M. Trujillo Administration Complex, Board Room",
        "address": "505 S. Main St., Suite 249, Las Cruces, NM 88001",
    }


def test_links():
    assert parsed_items[0]["links"] == [
        {
            "href": "https://aptg.co/HQGRfJ",
            "title": "Legal Notice",
        },
        {
            "href": "https://aptg.co/r5gYHh",
            "title": "Legal Notice (Spanish)",
        },
        {
            "href": "https://aptg.co/np0pGM",
            "title": "Agenda",
        },
        {
            "href": "https://aptg.co/b33FfC",
            "title": "Agenda (Spanish)",
        },
        {
            "href": "https://aptg.co/cJv9hG",
            "title": "Packet",
        },
        {
            "href": "https://lcpsnm.granicus.com/MediaPlayer.php?view_id=1&clip_id=281",  # noqa
            "title": "Video",
        },
        {
            "href": "https://lcpsnm.granicus.com/AgendaViewer.php?view_id=1&clip_id=281",  # noqa
            "title": "Agenda (Granicus)",
        },
    ]


def test_source():
    assert parsed_items[0]["source"] == "https://www.lcps.net/page/board-documents-new"


def test_links_have_href_and_title():
    for link in parsed_items[0]["links"]:
        assert "href" in link
        assert "title" in link


def test_video_link():
    video_items = [
        i for i in parsed_items if any(l["title"] == "Video" for l in i["links"])
    ]
    assert len(video_items) > 0


def test_status():
    assert parsed_items[0]["status"] == "passed"


def test_id():
    assert parsed_items[0]["id"].startswith("lascruc_public_boe/")


@pytest.mark.parametrize("item", parsed_items)
def test_all_day(item):
    assert item["all_day"] is False


@pytest.mark.parametrize("item", parsed_items)
def test_start_not_none(item):
    assert item["start"] is not None


@pytest.mark.parametrize("item", parsed_items)
def test_links_format(item):
    for link in item["links"]:
        assert isinstance(link["href"], str)
        assert isinstance(link["title"], str)
