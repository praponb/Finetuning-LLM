from pyspark.sql import functions as F

base = df_products.filter((F.col("price") >= 330) & (F.col("cost") <= 190))

agg = (
    base
    .groupBy("category")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    agg
    .filter(F.col("stock_qty_count") >= 6)
    .select("category", "max_cost")
    .orderBy("max_cost")
)
