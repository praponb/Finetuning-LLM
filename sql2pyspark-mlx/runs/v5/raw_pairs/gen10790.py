from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-08-18", "2025-02-01"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
    )
)

result = (
    valid
    .withColumn("running_count_monthly_fee", F.count("monthly_fee").over(w))
    .withColumn("running_min_monthly_fee", F.min("monthly_fee").over(w))
    .withColumn("running_max_monthly_fee", F.max("monthly_fee").over(w))
    .withColumn("running_sum_monthly_fee", F.sum("monthly_fee").over(w))
)
