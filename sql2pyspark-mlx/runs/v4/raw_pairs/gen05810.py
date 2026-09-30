from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "status")
    .subtract(
        df_subscriptions
        .filter(~F.col("status").isin(["EXPIRED", "ACTIVE"]))
        .select("id", "status")
    )
)
