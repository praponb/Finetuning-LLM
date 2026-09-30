from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan", "id")
    .subtract(
        df_subscriptions
        .filter(~F.col("status").isin(["EXPIRED", "ACTIVE", "CANCELLED"]))
        .select("plan", "id")
    )
)
