from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("status")
    .intersectAll(
        df_shipments
        .filter(F.col("shipped_date").between("2023-09-02", "2025-07-26"))
        .select("status")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("carrier")
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.min("cost").alias("min_cost"),
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
    )
    .orderBy("dhl_events")
)
