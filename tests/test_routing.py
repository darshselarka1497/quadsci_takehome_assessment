"""Region -> channel routing, including the unknown_region path."""
from app.config import Settings


def test_known_regions_route_to_channels():
    s = Settings(region_channel_map={"AMER": "amer-risk-alerts", "EMEA": "emea-risk-alerts"})
    assert s.channel_for_region("AMER") == "amer-risk-alerts"
    assert s.channel_for_region("EMEA") == "emea-risk-alerts"


def test_missing_or_unknown_region_is_unroutable():
    s = Settings(region_channel_map={"AMER": "amer-risk-alerts"})
    assert s.channel_for_region(None) is None      # missing
    assert s.channel_for_region("") is None         # empty
    assert s.channel_for_region("LATAM") is None    # not in config -> no default
