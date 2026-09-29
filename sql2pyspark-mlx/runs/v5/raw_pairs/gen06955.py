from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

scored = (
    df_sessions
    .filter(F.col("started_at") >= "2025-04-20")
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "browser", "started_at")
)

base = (
    ranked
    .filter(F.col("browser").isNotNull())
    .withColumn("clean_browser", F.trim("browser"))
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "clean_browser",
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("started_at").desc(), "min_subscription_monthly_fee")
)
