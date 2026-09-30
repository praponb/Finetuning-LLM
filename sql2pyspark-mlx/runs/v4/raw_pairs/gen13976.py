from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(
        F.count("*").alias("total_count"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
)
