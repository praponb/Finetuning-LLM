from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())
w3 = Window.orderBy(F.col("monthly_fee").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-06-22", "2024-12-01"))
    .select("*", F.upper("plan").alias("plan_upper"))
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.rank().over(w2).alias("monthly_fee_rank_2"),
        F.ntile(4).over(w3).alias("monthly_fee_bucket"),
    )
)
