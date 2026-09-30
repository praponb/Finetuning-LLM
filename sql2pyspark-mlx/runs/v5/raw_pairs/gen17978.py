from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 18, 19, 26]))
    .select(F.col("unit_price").alias("value"))
    .intersect(
        df_products
        .filter(F.col("brand") == "Stark")
        .select(F.col("price").alias("value"))
    )
    .orderBy("value")
)
