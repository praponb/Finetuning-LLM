from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("subscription_plan").orderBy(F.col("end_date").desc_nulls_last(), "id", "subscription_id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-08-01"))) & F.col("customer_id").isNotNull())
    .withColumn("adjusted_monthly_fee", F.round((F.col("monthly_fee") * 0.9), 2))
)

matched = (
    cleaned
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

ranked = (
    matched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
    )
    .orderBy(F.col("start_date").desc())
)
