from pyspark.sql import functions as F

matched_keys = (
    df_shipments
    .select("order_id")
    .exceptAll(
        df_shipments
        .filter(F.year("delivered_date") >= 2024)
        .select("order_id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("cost") >= 77, "LARGE").otherwise("SMALL").alias("cost_tier"),
        F.date_trunc("quarter", "shipped_date").alias("quarter_start"),
        "carrier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
    )
    .orderBy("cnt")
)
