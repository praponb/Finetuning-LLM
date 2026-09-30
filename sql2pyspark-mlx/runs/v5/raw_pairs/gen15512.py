from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isNotNull())
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
    )
)
