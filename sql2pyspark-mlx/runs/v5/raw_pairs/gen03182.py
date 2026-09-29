from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .withColumn("stock_qty_minus_cost", (F.col("stock_qty") - F.col("cost")))
    .withColumn("is_new", F.array_contains("tags", "new"))
)
