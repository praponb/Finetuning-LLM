from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("end_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("city").orderBy("last3_count_credit_limit", "id").rowsBetween(-2, -1)
w_ordered = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("status").orderBy(F.col("last3_max_credit_limit_avg_3").asc_nulls_last(), "id")

enriched = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.start_date",
        "src.end_date",
        "src.monthly_fee",
        "src.status",
        F.col("d.signup_date"),
        F.col("d.city"),
        F.col("d.credit_limit"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .filter(F.year("start_date") == 2025)
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "city",
        "credit_limit",
        "row_num",
        F.max("credit_limit").over(w2).alias("last3_max_credit_limit"),
        F.sum("credit_limit").over(w2).alias("last3_sum_credit_limit"),
        F.count("credit_limit").over(w2).alias("last3_count_credit_limit"),
        F.min("monthly_fee").over(w3).alias("status_min_monthly_fee"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("status_min_monthly_fee").over(w4).alias("status_min_monthly_fee_avg_2_prior"),
        F.avg("last3_max_credit_limit").over(w_ordered).alias("last3_max_credit_limit_avg_3"),
    )
)

ranked_subscriptions = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked_subscriptions
    .select(
        "id",
        "status",
        "city",
        "credit_limit",
        "last3_max_credit_limit",
        "status_min_monthly_fee",
        "status_min_monthly_fee_avg_2_prior",
        "dedup_rank",
    )
    .orderBy("credit_limit")
)
