from pyspark.sql import functions as F

src = df_order_items.filter(F.col("product_id").isin([11, 24, 33]))

agg = (
    src
    .groupBy("order_id", "product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    agg
    .filter(F.col("total_unit_price") >= 2200)
    .select("order_id", "total_unit_price")
    .orderBy(F.col("order_id").desc())
)
