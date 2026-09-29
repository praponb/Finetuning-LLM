from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("days_since_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("days_since_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-06-22")))
    .select(
        "*",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

result = (
    filtered
    .withColumn("running_min_monthly_fee", F.min("monthly_fee").over(w_part))
    .withColumn("running_sum_monthly_fee", F.sum("monthly_fee").over(w_part))
    .withColumn("running_count_monthly_fee", F.count("monthly_fee").over(w_part))
    .withColumn("running_max_monthly_fee", F.max("monthly_fee").over(w_part))
    .withColumn("running_days_since_start_date", F.sum("days_since_start_date").over(w2))
)
