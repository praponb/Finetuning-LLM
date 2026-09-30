from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 11, 18, 23]))
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") <= "2025-02-01")
        .select("customer_id")
    )
    .orderBy(F.col("customer_id").desc())
)
