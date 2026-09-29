from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "enterprise")
    .select("monthly_fee")
    .subtract(df_shipments.select(F.col("cost").alias("monthly_fee")))
)
