from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy("started_at", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

ranked = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "device", "browser", "started_at", "country")
)

result = (
    ranked
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").rlike("^(Safari|Firefox)$"), "EXTENDED").otherwise("CORE").alias("browser_zone"),
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("max_subscription_monthly_fee").desc())
)
