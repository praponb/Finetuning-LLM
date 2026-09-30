from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

cleaned = (
    valid
    .filter(F.col("start_date") > F.lit("2023-04-08").cast("date"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "monthly_fee_whole",
        "status_prefix",
        F.round((F.col("monthly_fee_whole") / F.col("monthly_fee")), 2).alias("monthly_fee_whole_per_monthly_fee"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        "monthly_fee_whole",
        F.max("monthly_fee").over(w).alias("last3_max_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last3_avg_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last3_sum_monthly_fee"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.monthly_fee",
        "src.status",
        "src.monthly_fee_whole",
        "src.last3_max_monthly_fee",
        "src.last3_sum_monthly_fee",
        F.col("p.city"),
    )
)

result = (
    joined
    .select("id", "city", "monthly_fee", "last3_sum_monthly_fee")
    .unpivot(
        ids=["id", "city"],
        values=["monthly_fee", "last3_sum_monthly_fee"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
