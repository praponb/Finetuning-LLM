from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w_part = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

scored = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("row_seq_2", F.row_number().over(w_part))
)

result = (
    scored
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "row_seq",
        "event_seq",
        "row_seq_2",
        F.col("sum_subscription_monthly_fee"),
    )
)
