from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("is_premium").desc_nulls_first(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w3 = Window.orderBy("is_premium", "id")
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.orderBy(F.col("is_premium").desc_nulls_first())
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("status").orderBy("is_premium_dense_rank", "id")
w8 = Window.orderBy("monthly_fee", "id")
w9 = Window.partitionBy("plan").orderBy(F.col("is_premium_dense_rank").desc_nulls_last(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isin([5, 26, 29]))
    .select(
        "*",
        F.when(F.col("monthly_fee") >= 243, "High").when(F.col("monthly_fee") >= 103, "Medium").otherwise("Low").alias("monthly_fee_tier"),
        F.when(F.col("plan") == "premium", 1).otherwise(0).alias("is_premium"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(3).over(w4).alias("monthly_fee_bucket"),
        F.dense_rank().over(w5).alias("is_premium_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w6)), 4).alias("monthly_fee_share"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.row_number().over(w7).alias("row_seq"),
        F.round((F.col("is_premium_dense_rank") / F.sum("is_premium_dense_rank").over(w6)), 4).alias("is_premium_dense_rank_share"),
        F.ntile(4).over(w8).alias("monthly_fee_bucket_2"),
    )
)

deduped_subscriptions = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    deduped_subscriptions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.start_date") >= F.col("sub.start_date")) & (F.col("s.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    attributed
    .select(
        "id",
        "subscription_id",
        "plan",
        F.col("monthly_fee").alias("MONTHLY_FEE_VAL"),
        F.col("monthly_fee_share").alias("MONTHLY_FEE_SHARE"),
        F.col("is_premium_dense_rank_share").alias("IS_PREMIUM_DENSE_RANK_SHARE"),
        "subscription_monthly_fee",
    )
    .unpivot(
        ["id", "subscription_id", "plan"],
        ["MONTHLY_FEE_VAL", "MONTHLY_FEE_SHARE", "IS_PREMIUM_DENSE_RANK_SHARE", "subscription_monthly_fee"],
        "metric",
        "reading",
    )
)
