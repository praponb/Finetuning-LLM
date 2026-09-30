from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "created_at", F.col("line.product_id"), F.col("line.qty"))
)

result = (
    lines
    .groupBy(
        F.when(F.col("qty") >= 5, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
    )
    .agg(
        F.avg("qty").alias("avg_qty"),
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min_2"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
