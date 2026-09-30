from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select(F.col("price").alias("value"), F.lit("product").alias("record_type"))
    .unionByName(
        df_orders
        .filter(F.col("discount") > 0.37)
        .select(F.col("amount").alias("value"), F.lit("order").alias("record_type"))
    )
    .distinct()
)
