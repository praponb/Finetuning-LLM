from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_start_date_to_end_date").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("start_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id")

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
    .withColumn("plan_group", F.when(F.col("plan").isin(["basic", "premium"]), "Core").otherwise("Other"))
)

result = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w).alias("days_start_date_to_end_date_percent_rank"),
        F.lag("days_start_date_to_end_date", 2).over(w2).alias("prev_days_start_date_to_end_date"),
        F.ntile(3).over(w3).alias("monthly_fee_bucket"),
        F.row_number().over(w4).alias("seq_no"),
    )
)
