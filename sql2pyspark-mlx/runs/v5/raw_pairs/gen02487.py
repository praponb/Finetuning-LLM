from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 347, "high").when(F.col("stock_qty") >= 113, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 5)
)
