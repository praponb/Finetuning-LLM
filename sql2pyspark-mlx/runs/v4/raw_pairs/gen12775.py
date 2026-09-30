from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([26, 35]))
    .select(F.col("cost").alias("value"), F.lit("product").alias("source"))
    .unionByName(
        df_order_items
        .select(F.col("unit_price").alias("value"), F.lit("order_item").alias("source"))
    )
    .orderBy(F.col("value").desc())
)
