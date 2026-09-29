from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date", F.lit("store_sale").alias("source"))
    .union(
        df_orders
        .filter(F.col("shipping_fee") > 27)
        .select("order_date", F.lit("order").alias("source"))
    )
    .distinct()
)
