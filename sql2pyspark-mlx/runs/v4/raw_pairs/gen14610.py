from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("status") != "ACTIVE")
        .select("id")
    )
)
