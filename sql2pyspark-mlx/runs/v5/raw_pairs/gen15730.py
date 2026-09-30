from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .select(
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("p.price"),
        F.add_months(F.col("p.launch_date"), 3).alias("launch_date_plus_months"),
    )
)
