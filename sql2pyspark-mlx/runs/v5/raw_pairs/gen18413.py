from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc_nulls_first())
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_first(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("country").eqNullSafe(F.lit("JP"))))
    .withColumn("browser_lower", F.lower("browser"))
)

deduped = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

latest = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "browser_lower",
        "row_num",
        "dedup_rank",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy("id", F.col("started_at").desc())
    .limit(20)
)
