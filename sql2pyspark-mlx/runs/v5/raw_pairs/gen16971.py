from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

with_windows = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        F.cume_dist().over(w).alias("monthly_fee_cume_dist"),
        F.ntile(3).over(w2).alias("monthly_fee_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

valid = (
    latest
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE"]))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "monthly_fee_cume_dist",
        "monthly_fee_bucket",
        "row_num",
        F.floor("monthly_fee_bucket").alias("monthly_fee_bucket_whole"),
        F.ceil("monthly_fee_bucket").alias("monthly_fee_bucket_whole_2"),
    )
)

cleaned = (
    valid
    .filter(F.col("monthly_fee_bucket_whole_2") > 39)
    .select(
        "*",
        F.floor("monthly_fee_bucket_whole_2").alias("monthly_fee_bucket_whole_2_whole"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "monthly_fee_cume_dist",
        "monthly_fee_bucket",
        "row_num",
        "monthly_fee_bucket_whole",
        F.when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(20)
)
