from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") > 284) & (F.col("cost") < 240) & (F.col("stock_qty") != 349))
    .groupBy("brand", "category")
    .agg(F.countDistinct("stock_qty").alias("unique_suppliers"))
)
