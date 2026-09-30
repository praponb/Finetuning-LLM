from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc(), "id")
w_ordered = Window.partitionBy("status").orderBy("credit_limit", "id")
w3 = Window.orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("city", "plan").orderBy(F.col("rn").desc(), "id")

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.end_date",
        "src.monthly_fee",
        "src.status",
        F.col("p.signup_date"),
        F.col("p.credit_limit"),
        F.col("p.city"),
    )
)

valid = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.lead("credit_limit").over(w_ordered).alias("next_credit_limit"),
        F.lag("credit_limit").over(w_ordered).alias("prev_credit_limit"),
        F.lead("status_length").over(w_ordered).alias("next_status_length"),
        F.lag("status_length").over(w_ordered).alias("prev_status_length"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_2"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

with_window = (
    latest
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.end_date") >= F.col("sub.start_date")) & (F.col("e.end_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    with_window
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.segment").alias("segment"),
        F.col("e.city").alias("subscription_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.monthly_fee")).alias("total_monthly_fee"),
    )
)
