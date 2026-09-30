from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("product_id").isin([2, 4])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.cost") >= 560)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
    .orderBy(F.col("product_name").desc(), F.col("stock_qty").desc())
)
