from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Acme", "Globex"]))
    .select(F.lit("product").alias("source"), F.col("cost").alias("value"))
    .unionByName(
        df_subscriptions
        .select(F.lit("subscription").alias("source"), F.col("monthly_fee").alias("value"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
