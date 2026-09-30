from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.year("sale_date") == 2023)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

remaining_keys = (
    valid
    .select("store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id") == 4)
        .select("store_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

valid_store_sales = (
    reconciled
    .filter((F.col("sale_date") > F.lit("2024-01-07").cast("date")) & (F.col("revenue") > 1200))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "revenue_int",
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)

result = (
    valid_store_sales
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3400, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["store_id", "revenue_tier"], ["store_id"], []],
        "store_id",
        "revenue_tier",
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.stddev_samp("revenue").alias("revenue_stddev_2"),
        F.max("revenue_int").alias("max_revenue_int"),
        F.grouping("store_id").alias("gid"),
    )
    .orderBy("store_id")
)
