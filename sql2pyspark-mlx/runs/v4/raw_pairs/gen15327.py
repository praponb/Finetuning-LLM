from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") > 268) & (F.col("price") > 470))
    .agg(F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"))
)
