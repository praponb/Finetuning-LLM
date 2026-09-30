from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.price"),
        F.when(F.col("p.price") >= 1050, "High").when(F.col("p.price") >= 370, "Medium").otherwise("Low").alias("price_level"),
    )
    .orderBy("price_level")
)
