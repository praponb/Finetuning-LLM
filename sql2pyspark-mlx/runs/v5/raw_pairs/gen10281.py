from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .select("end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("end_date")
    )
    .distinct()
)
