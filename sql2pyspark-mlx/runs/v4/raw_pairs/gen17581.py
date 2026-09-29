from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w_part = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w_part))
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w3))
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "start_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "end_date"))
    .groupingSets(
        [["activity_month", "year_start"], ["activity_month"]],
        "activity_month",
        "year_start",
    )
    .agg(
        F.percentile_approx("monthly_fee_bucket", [0.25, 0.5, 0.75]).alias("monthly_fee_bucket_quartiles"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee_bucket"))).alias("paused_total_monthly_fee_bucket"),
        F.grouping("activity_month").alias("gid"),
    )
    .filter(F.col("paused_total_monthly_fee_bucket") >= 0)
    .orderBy(F.col("activity_month").desc())
)
