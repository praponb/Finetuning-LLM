from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
        .select("status")
    )
)
