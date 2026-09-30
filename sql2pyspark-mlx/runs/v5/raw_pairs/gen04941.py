from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w_ordered = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("last_known_monthly_fee").desc_nulls_last(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee_bucket").desc(), "id")

with_windows = (
    df_subscriptions
    .select(
        "*",
        F.ntile(5).over(w_recent).alias("monthly_fee_bucket"),
        F.last("monthly_fee", ignorenulls=True).over(w_ordered).alias("last_known_monthly_fee"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_bucket",
        "rank_in_group",
    )
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

wide = (
    latest
    .groupBy("plan")
    .pivot("status", ["PAUSED", "EXPIRED"])
    .agg(F.avg("monthly_fee_bucket").alias("value_sum"))
    .select("plan", F.col("PAUSED").alias("paused"), F.col("EXPIRED").alias("expired"))
)

result = wide.select("plan", "paused", "expired")
