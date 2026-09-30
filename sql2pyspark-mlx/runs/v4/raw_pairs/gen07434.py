from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.year(F.col("p.launch_date")) == 2024)
    .groupBy(F.col("p.supplier_id"), F.col("p.category"))
    .agg(F.count("*").alias("num_inventory"))
    .filter(F.col("num_inventory") > 2)
    .orderBy(F.col("num_inventory").desc())
)
