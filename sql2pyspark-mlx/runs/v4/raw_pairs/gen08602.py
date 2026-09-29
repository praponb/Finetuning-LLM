from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_start_date_to_end_date").desc())
w2 = Window.partitionBy("country").orderBy("days_start_date_to_end_date", "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.name"), F.col("b.country"))
)

with_windows = (
    enriched
    .withColumn("days_start_date_to_end_date_dense_rank", F.dense_rank().over(w))
    .withColumn("days_start_date_to_end_date_bucket", F.ntile(5).over(w2))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("days_start_date_to_end_date_bucket") >= 8, "HIGH").otherwise("LOW").alias("days_start_date_to_end_date_bucket_tier"),
        "customer_id",
    )
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("monthly_fee_per_cancelled"),
        F.round(F.avg("days_start_date_to_end_date"), 2).alias("avg_days_start_date_to_end_date"),
        F.sum(F.when(F.col("country") == "DE", F.col("days_start_date_to_end_date")).otherwise(0)).alias("de_days_start_date_to_end_date"),
        F.percentile_approx("days_start_date_to_end_date_dense_rank", [0.25, 0.5, 0.75]).alias("days_start_date_to_end_date_dense_rank_quartiles"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .orderBy(F.col("de_days_start_date_to_end_date").desc())
)
