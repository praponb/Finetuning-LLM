from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("monthly_fee", "id")
w_recent = Window.partitionBy("customer_id").orderBy("start_date", "id")
w3 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee").asc_nulls_last())

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.lead("monthly_fee", 1).over(w_hist).alias("next_monthly_fee"),
        F.lead("monthly_fee", 1).over(w_recent).alias("next_monthly_fee_2"),
        F.dense_rank().over(w3).alias("monthly_fee_dense_rank"),
    )
)
