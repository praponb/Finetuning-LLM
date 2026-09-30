from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") == 222) & (F.col("stock_qty") <= 273) & (F.length("product_name") >= 14))
    .groupBy("category")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
        F.avg("price").alias("avg_price"),
    )
)
