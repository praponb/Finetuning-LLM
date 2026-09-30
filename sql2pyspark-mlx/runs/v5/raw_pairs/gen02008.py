from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("category") != "Fashion").alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.year(F.col("ss.sale_date")) == 2023)
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("ss.units"),
    )
)
