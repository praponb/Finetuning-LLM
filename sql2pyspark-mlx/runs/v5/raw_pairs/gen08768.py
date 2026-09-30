from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
    .filter(F.col("stock_qty_range") < 61)
    .orderBy(F.col("globex_count").desc())
)
