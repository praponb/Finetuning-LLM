from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([11, 19]))
    .select("id", "brand", F.least("cost", "price").alias("least_cost_price"))
    .orderBy(F.col("least_cost_price").desc(), "brand")
)
