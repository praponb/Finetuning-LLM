from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("amount", "id")
w2 = Window.partitionBy("order_id", "status")
w_grp = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_first())

enriched = (
    df_payments
    .withColumn("amount_bucket", F.ntile(10).over(w_hist))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("amount_dense_rank", F.dense_rank().over(w_grp))
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("month", "paid_at"))
    .withColumn("week_start", F.date_trunc("week", "paid_at"))
    .groupingSets(
        [["activity_month", "method", "week_start"], ["activity_month", "method"]],
        "activity_month",
        "method",
        "week_start",
    )
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount_share")).otherwise(0)).alias("paypal_amount_share"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("amount_per_success"),
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
    )
    .filter(F.col("paypal_amount_share") < 0)
)
