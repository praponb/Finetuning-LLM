from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(~F.col("plan").isin(["premium", "standard"]))
        .select("plan")
    )
)
