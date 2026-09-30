from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 160)
    .select(F.lit("subscription").alias("origin"), "start_date")
    .union(
        df_store_sales
        .filter(F.col("units") <= 39)
        .select(F.lit("store_sale").alias("origin"), "sale_date")
    )
    .distinct()
    .orderBy("start_date")
)
