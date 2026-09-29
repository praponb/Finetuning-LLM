from pyspark.sql import functions as F

matched_keys = (
    df_shipments
    .select("order_id", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("delivered_date") <= "2023-03-01")
        .select("order_id", "id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.when(F.col("cost") >= 83, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.count_if(F.col("carrier") == "Kerry").alias("kerry_events"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost"))).alias("fedex_total_cost"),
    )
    .filter(F.col("cost_p95") > 0)
)
