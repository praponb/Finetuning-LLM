from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isin([24, 36, 37]) & ~F.col("c.country").isin(["TH", "US"]))
    .select(
        F.col("s.plan"),
        F.col("s.monthly_fee"),
        F.col("s.start_date"),
        F.col("c.segment"),
        F.concat_ws("-", F.col("s.plan"), F.col("s.status")).alias("plan_status_key"),
    )
    .orderBy(F.col("monthly_fee").desc(), "plan")
)
