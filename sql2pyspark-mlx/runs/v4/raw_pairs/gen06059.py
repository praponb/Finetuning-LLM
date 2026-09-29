from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("status", "order_id")
    .intersectAll(
        df_shipments
        .filter(F.col("status") != "LOST")
        .select("status", "order_id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("cost") >= 78, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "carrier",
    )
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost"))).alias("delivered_total_cost"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
    )
    .orderBy(F.col("carrier").desc_nulls_first())
)
