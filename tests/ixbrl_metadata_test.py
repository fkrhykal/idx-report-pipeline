from idx.defs.assets import EntryPoint, parse_ixbrl_financial_report_metadata


def test_parse_ixbrl_financial_report_metadata():
    with open("tests/fixtures/ADES_2022_TW3.zip", "rb") as f:
        metadata = parse_ixbrl_financial_report_metadata(f.read())
        assert metadata is not None
        assert metadata.EntryPoint == EntryPoint.GENERAL
