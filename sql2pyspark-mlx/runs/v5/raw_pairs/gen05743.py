from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("monthly_fee_avg_5")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("start_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.orderBy(F.col("start_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)

scored = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "CANCELLED"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_5"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
    )
)

cleaned = (
    scored
    .filter(F.col("start_date") >= F.lit("2023-09-01").cast("date"))
    .withColumn("start_date_label", F.date_format("start_date", "yyyy-MM-dd"))
    .withColumn("start_date_period", F.date_trunc("week", "start_date"))
)

kept_keys = (
    cleaned
    .select("id")
    .intersect(
        df_subscriptions
        .filter(~F.col("status").isin(["ACTIVE", "EXPIRED", "PAUSED"]))
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "last_known_monthly_fee",
        "start_date_label",
        "start_date_period",
        F.percent_rank().over(w3).alias("monthly_fee_avg_5_percent_rank"),
        F.sum("last_known_monthly_fee").over(w4).alias("customer_id_sum_last_known_monthly_fee"),
        F.max("monthly_fee_avg_5").over(w5).alias("monthly_fee_avg_5_max_30d"),
        F.dense_rank().over(w3).alias("monthly_fee_avg_5_dense_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "last_known_monthly_fee",
        "start_date_label",
        "start_date_period",
        "customer_id_sum_last_known_monthly_fee",
        "monthly_fee_avg_5_max_30d",
        "monthly_fee_avg_5_dense_rank",
        F.count("monthly_fee_avg_5_percent_rank").over(w6).alias("monthly_fee_avg_5_percent_rank_count_7d"),
    )
)

result = (
    with_windows
    .select(
        "start_date_label",
        "start_date_period",
        "customer_id_sum_last_known_monthly_fee",
        "monthly_fee_avg_5_max_30d",
        "monthly_fee_avg_5_dense_rank",
    )
    .orderBy("customer_id_sum_last_known_monthly_fee")
)
