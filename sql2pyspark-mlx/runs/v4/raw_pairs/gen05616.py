from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last())
w_recent = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("end_date", "id")
w4 = Window.partitionBy("status", "plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_first(), "id")
w6 = Window.partitionBy("status", "plan").orderBy("start_date", "id")
w7 = Window.partitionBy("customer_id")
w8 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())

ranked = (
    df_subscriptions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w_recent))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 1).over(w_part))
    .withColumn("last_known_monthly_fee_2", F.last("monthly_fee", ignorenulls=True).over(w4))
)

ranked_subscriptions = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked_subscriptions
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w6).alias("prev_monthly_fee_2"),
        F.round((F.col("last_known_monthly_fee_2") / F.sum("last_known_monthly_fee_2").over(w7)), 4).alias("last_known_monthly_fee_2_share"),
        F.cume_dist().over(w8).alias("monthly_fee_cume_dist"),
    )
)

valid = (
    with_windows
    .filter((F.col("end_date") >= F.lit("2023-05-18").cast("date")) & (F.col("last_known_monthly_fee") < 110))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "last_known_monthly_fee",
        "prev_monthly_fee",
        "rn",
        "prev_monthly_fee_2",
        "last_known_monthly_fee_2_share",
        F.least("last_known_monthly_fee", "monthly_fee").alias("least_last_known_monthly_fee_monthly_fee"),
        F.col("monthly_fee_cume_dist").cast("int").alias("monthly_fee_cume_dist_int"),
    )
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "end_date",
        "last_known_monthly_fee",
        "prev_monthly_fee",
        "rn",
        "last_known_monthly_fee_2_share",
        "least_last_known_monthly_fee_monthly_fee",
        "monthly_fee_cume_dist_int",
    )
    .orderBy(F.col("last_known_monthly_fee").desc_nulls_first())
)
