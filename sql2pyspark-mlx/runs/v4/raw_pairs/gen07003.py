from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2024)
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(
        df_store_sales
        .filter(F.year("sale_date") == 2024)
        .select("revenue", F.lit("store_sale").alias("source"))
    )
)
