from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("segment")
w3 = Window.orderBy("is_us", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("is_us", "id")

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-04-18").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        "age",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

metrics = (
    valid
    .filter(F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "is_active",
        "age",
        "is_us",
        F.avg("is_us").over(w).alias("is_us_avg_5_prior"),
        F.max("is_us").over(w2).alias("segment_max_is_us"),
    )
)

enriched = (
    metrics
    .withColumn("cumulative_age", F.sum("age").over(w3))
    .withColumn("is_us_bucket", F.ntile(10).over(w4))
)

eligible = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (~F.col("c.status").isin(["ACTIVE", "CANCELLED", "EXPIRED"])), "left_anti")
)

eligible_customers = (
    eligible
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.country").isin(["US", "JP"])), "left_anti")
)

result = (
    eligible_customers
    .groupBy(
        F.when(F.col("is_us_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("is_us_bucket_tier"),
        F.when(F.col("is_us_avg_5_prior") >= 75, "PREMIUM").otherwise("STANDARD").alias("is_us_avg_5_prior_tier"),
        F.when(F.col("age") >= 72, "LARGE").otherwise("SMALL").alias("age_tier"),
    )
    .agg(
        F.round(F.avg("segment_max_is_us"), 2).alias("avg_segment_max_is_us"),
        F.avg("is_us").alias("avg_is_us"),
        F.count("*").alias("row_count"),
        F.bit_or("age").alias("age_bit_or"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("age_bit_or")
)
