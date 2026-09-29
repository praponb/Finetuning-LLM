from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at")
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc_nulls_last(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2024-02-22 00:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.trim("browser").alias("clean_browser"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

latest_sessions = (
    ranked
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "country_clean",
        "rn",
        F.when(F.col("country").rlike("^(UK|US)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("country_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("rn").desc_nulls_last())
)
