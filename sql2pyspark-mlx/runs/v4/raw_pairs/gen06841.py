from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee_filled").desc_nulls_first(), "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-08-08").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
)

cleaned = (
    valid
    .filter(F.col("end_date") >= F.lit("2023-12-31").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "monthly_fee_filled",
        F.trim("status").alias("clean_status"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("YEAR", "start_date").alias("year_start"),
        F.when(F.col("monthly_fee_filled") >= 70, "HIGH").otherwise("LOW").alias("monthly_fee_filled_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee_filled")).otherwise(0)).alias("expired_monthly_fee_filled"),
        F.percentile_approx("monthly_fee_filled", [0.25, 0.5, 0.75]).alias("monthly_fee_filled_quartiles"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("monthly_fee_filled", [0.25, 0.5, 0.75]).alias("monthly_fee_filled_quartiles_2"),
        F.percentile_approx("monthly_fee_filled", 0.95).alias("monthly_fee_filled_p95"),
        F.avg(F.when(F.col("plan") == "enterprise", F.col("monthly_fee_filled"))).alias("enterprise_avg_monthly_fee_filled"),
    )
    .orderBy(F.col("enterprise_avg_monthly_fee_filled").desc())
)
