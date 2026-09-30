from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(111, 185))
    .groupBy("brand")
    .agg(
        F.min("price").alias("min_price"),
        F.min("cost").alias("min_cost"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
    .filter(F.col("min_price") > 470)
)
