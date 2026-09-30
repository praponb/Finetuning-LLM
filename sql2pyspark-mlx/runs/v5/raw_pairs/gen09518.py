from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "premium", "basic"]))
    .select("plan")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isin([6, 18]))
        .select("plan")
    )
)
