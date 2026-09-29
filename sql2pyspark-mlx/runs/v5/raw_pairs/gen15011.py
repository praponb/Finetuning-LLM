from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan", "customer_id")
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first())
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w6 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_max_monthly_fee").asc_nulls_first())

metrics = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.max("monthly_fee").over(w).alias("customer_id_max_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("plan_avg_monthly_fee"),
        F.rank().over(w4).alias("monthly_fee_rank"),
        F.ntile(10).over(w5).alias("monthly_fee_bucket"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.segment"))
)

base = (
    enriched
    .filter((F.col("start_date") >= F.lit("2023-09-16").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("monthly_fee_rank") >= 8, "High").when(F.col("monthly_fee_rank") >= 3, "Medium").otherwise("Low").alias("monthly_fee_rank_tier"),
        F.concat_ws("-", "plan", "status").alias("plan_status_key"),
    )
)

ranked = (
    base
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "status",
        "customer_id_max_monthly_fee",
        "plan_avg_monthly_fee",
        "monthly_fee_rank",
        "segment",
        "monthly_fee_rank_tier",
    )
)

enriched_subscriptions = (
    ranked
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.customer_id_max_monthly_fee",
        "a.plan_avg_monthly_fee",
        "a.monthly_fee_rank",
        "a.segment",
        F.col("b.city"),
    )
)

result = (
    enriched_subscriptions
    .select("id", "customer_id_max_monthly_fee", "plan_avg_monthly_fee", "segment")
)
