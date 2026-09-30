from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w3))
)

result = (
    with_windows
    .groupBy(F.date_trunc("quarter", "end_date").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee_share")).otherwise(0)).alias("paused_monthly_fee_share"),
        F.max(F.when(F.col("status") == "PAUSED", F.col("monthly_fee_bucket"))).alias("paused_max_monthly_fee_bucket"),
        F.max(F.when(F.col("status") == "ACTIVE", F.col("row_seq"))).alias("active_max_row_seq"),
    )
    .orderBy(F.col("active_max_row_seq").desc_nulls_first())
)
