from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("delivered_date").between("2023-10-01", "2025-09-01"))
        .select("id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 84, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "cost_tier"], ["quarter_start"]],
        "quarter_start",
        "cost_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost"))).alias("returned_total_cost"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
)
