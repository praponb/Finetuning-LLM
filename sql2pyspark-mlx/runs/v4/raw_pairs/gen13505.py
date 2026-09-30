from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2024-01-10"))) & (F.col("delivered_date") > "2024-04-01"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        F.upper("carrier").alias("carrier_upper"),
    )
)

kept_keys = (
    cleaned
    .select("status", "id")
    .subtract(
        df_shipments
        .filter(F.col("carrier") == "FedEx")
        .select("status", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("status").isin(["RETURNED", "LOST"]))
    .select(
        "*",
        F.date_add("shipped_date", 41).alias("shipped_date_plus_41d"),
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)

grouped = (
    filtered
    .groupBy(F.when(F.col("cost") >= 97, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.count("*").alias("event_count"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles_2"),
        F.stddev_samp("cost").alias("cost_stddev_2"),
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
)

result = (
    grouped
    .select(
        "cost_tier",
        "cost_stddev",
        "cost_quartiles",
        "cost_quartiles_2",
        "cost_stddev_2",
        "lost_cost",
    )
    .orderBy(F.col("cost_tier").desc_nulls_last())
    .limit(20)
)
