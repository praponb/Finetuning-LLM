from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status")
w_grp = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee_share").desc(), "id")

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-06-19").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.upper("status").alias("status_upper"))
)

enriched = (
    valid
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.ntile(3).over(w3).alias("monthly_fee_bucket"),
    )
)

scored = (
    enriched
    .withColumn("status_min_next_monthly_fee", F.min("next_monthly_fee").over(w4))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

valid_subscriptions = (
    scored
    .filter((F.col("start_date") >= F.lit("2023-09-16").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.col("next_monthly_fee") / F.col("prev_monthly_fee")), 2).alias("next_monthly_fee_per_prev_monthly_fee"),
    )
)

result = (
    valid_subscriptions
    .select(
        "id",
        "start_date",
        "status",
        "next_monthly_fee",
        "prev_monthly_fee",
        "monthly_fee_share",
        "row_seq",
        "next_monthly_fee_per_prev_monthly_fee",
    )
)
