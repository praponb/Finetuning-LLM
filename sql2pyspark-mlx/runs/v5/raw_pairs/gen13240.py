from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.when(F.col("qty") >= 5, "LARGE").otherwise("SMALL").alias("qty_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.percentile_approx("price", 0.95).alias("price_p95"),
        F.percentile_approx("qty", 0.9).alias("qty_p90"),
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("ended_at"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("price_p95").asc_nulls_first())
)
