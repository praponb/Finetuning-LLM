from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "UPS")
        .select(F.col("cost").alias("monthly_fee"))
    )
    .distinct()
    .orderBy(F.col("monthly_fee").desc())
)
