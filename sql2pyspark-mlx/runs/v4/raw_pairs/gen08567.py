from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date_month_end", "id")
w2 = Window.partitionBy("status").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status").orderBy("latest_rank", "id")
w5 = Window.partitionBy("status").orderBy("monthly_fee", "id")

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-06-11").cast("date"))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("status").isin(["ACTIVE", "PAUSED"]), "Group A").otherwise("Other").alias("status_group"),
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

base_subscriptions = (
    base
    .filter(F.col("status").isin(["PAUSED", "ACTIVE", "CANCELLED"]))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "status_group",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

latest = (
    base_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("monthly_fee_bucket", F.ntile(3).over(w5))
)
