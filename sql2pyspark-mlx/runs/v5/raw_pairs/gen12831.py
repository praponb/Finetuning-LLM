from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .groupBy(F.col("suppliers.country"))
    .agg(
        F.count(F.col("products.cost")).alias("cost_count"),
        F.max(F.col("products.launch_date")).alias("last_launch_date"),
    )
    .orderBy("country")
)
