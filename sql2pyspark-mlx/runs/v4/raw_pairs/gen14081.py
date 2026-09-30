from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee_whole").desc(), "id")
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("monthly_fee_whole", "id")

valid = (
    df_subscriptions
    .filter(F.col("end_date") <= "2024-02-10")
    .withColumn("plan_lower", F.lower("plan"))
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
)

enriched = (
    valid
    .withColumn("monthly_fee_whole_avg_3", F.avg("monthly_fee_whole").over(w))
    .withColumn("monthly_fee_whole_bucket", F.ntile(10).over(w2))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w3))
    .withColumn("monthly_fee_whole_bucket_2", F.ntile(5).over(w4))
)

filtered = (
    enriched
    .filter(F.col("start_date") >= F.lit("2023-09-11").cast("date"))
    .select(
        "*",
        (F.col("monthly_fee_whole") / F.nullif("monthly_fee_whole_avg_3", F.lit(0))).alias("monthly_fee_whole_per_monthly_fee_whole_avg_3"),
    )
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        "plan_lower",
        "monthly_fee_whole_avg_3",
        "monthly_fee_whole_bucket_2",
        F.when(F.col("status").rlike("^(EXPIRED|CANCELLED)$"), "CORE").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
)
