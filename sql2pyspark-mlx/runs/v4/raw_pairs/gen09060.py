from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("store_id") == 5).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.stock_qty").between(356, 376))
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
    .orderBy("product_name")
)
