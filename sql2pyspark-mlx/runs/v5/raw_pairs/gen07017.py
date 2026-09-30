from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "started_at", F.col("avg_subscription_monthly_fee"))
    .orderBy("customer_id")
)
