from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan")
    .intersect(
        df_subscriptions
        .filter(~F.col("status").isin(["CANCELLED", "PAUSED"]))
        .select("plan")
    )
)
