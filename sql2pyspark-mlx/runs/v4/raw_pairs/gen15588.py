from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("units").between(17, 33)).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.launch_date") <= "2024-06-14")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("p.category"),
    )
)
