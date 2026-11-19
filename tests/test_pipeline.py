from src.main import run_pipeline


def test_end_to_end_pipeline_runs_across_all_sample_datasets(spark):
    results = run_pipeline(spark)

    # 5 flat-file rows + 3 AUS + 3 USA + 3 IND = 14 raw records
    assert results["raw"].count() == 14
    # 1 bad row quarantined (AUS_2 Jonnathan with invalid date 2021-13-13)
    assert results["quarantine"].count() == 1
    assert results["valid"].count() == 13
    assert results["staging"].count() == 13

    # All 5 target country tables created
    assert set(results["country_tables"].keys()) == {
        "Table_India",
        "Table_USA",
        "Table_Australia",
        "Table_Canada",
        "Table_Philippines",
    }

    # Flattened JSON has 2 transactions, joined back to all 13 valid members
    assert results["redemptions_flat"].count() == 2
    assert results["member_redemption_summary"].count() == 13