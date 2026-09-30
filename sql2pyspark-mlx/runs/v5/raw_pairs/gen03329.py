from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy(F.col("monthly_fee").desc())
w3 = Window.partitionBy("status").orderBy(F.col("status_max_monthly_fee").desc())
w4 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w5 = Window.partitionBy("customer_id")
w6 = Window.partitionBy("customer_id", "status")
w7 = Window.partitionBy("plan")
w8 = Window.partitionBy("plan", "customer_id")
w9 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_share").asc_nulls_last())

remaining_keys = (
    df_subscriptions
    .select("status", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") <= "2025-09-12")
        .select("status", "id")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = kept_rows.withColumn("status_max_monthly_fee", F.max("monthly_fee").over(w))

metrics = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
        F.dense_rank().over(w3).alias("status_max_monthly_fee_dense_rank"),
        F.dense_rank().over(w4).alias("monthly_fee_dense_rank_2"),
        F.sum("status_max_monthly_fee").over(w5).alias("customer_id_sum_status_max_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w6)), 4).alias("monthly_fee_share"),
    )
)

enriched_subscriptions = (
    metrics
    .filter(F.col("monthly_fee_dense_rank") == 5)
    .select(
        "id",
        "customer_id",
        "status_max_monthly_fee",
        "monthly_fee_dense_rank_2",
        "customer_id_sum_status_max_monthly_fee",
        "monthly_fee_share",
        F.min("monthly_fee").over(w).alias("status_min_monthly_fee"),
        F.round((F.col("status_max_monthly_fee_dense_rank") / F.sum("status_max_monthly_fee_dense_rank").over(w)), 4).alias("status_max_monthly_fee_dense_rank_share"),
        F.round((F.col("status_max_monthly_fee") / F.sum("status_max_monthly_fee").over(w7)), 4).alias("status_max_monthly_fee_share"),
        F.min("customer_id_sum_status_max_monthly_fee").over(w8).alias("plan_min_customer_id_sum_status_max_monthly_fee"),
        F.min("customer_id_sum_status_max_monthly_fee").over(w6).alias("customer_id_min_customer_id_sum_status_max_monthly_fee"),
    )
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .select(
        "id",
        "customer_id",
        "status_max_monthly_fee",
        "monthly_fee_share",
        "status_min_monthly_fee",
        "status_max_monthly_fee_dense_rank_share",
        "status_max_monthly_fee_share",
        "customer_id_min_customer_id_sum_status_max_monthly_fee",
        F.round((F.col("status_max_monthly_fee") / F.sum("status_max_monthly_fee").over(w5)), 4).alias("status_max_monthly_fee_share_2"),
        F.rank().over(w9).alias("monthly_fee_share_rank"),
        F.count("status_min_monthly_fee").over(w5).alias("customer_id_count_status_min_monthly_fee"),
    )
)

joined = (
    enriched_subscriptions_2
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.customer_id",
        "a.status_min_monthly_fee",
        "a.monthly_fee_share_rank",
        "a.customer_id_count_status_min_monthly_fee",
        F.col("b.credit_limit"),
        F.col("b.segment"),
        F.col("b.country"),
    )
)

result = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["customer_segment", "subscription_country"], ["customer_segment"], []],
        F.col("c.segment").alias("customer_segment"),
        F.col("src.country").alias("subscription_country"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.status_min_monthly_fee")).alias("max_status_min_monthly_fee"),
    )
)
