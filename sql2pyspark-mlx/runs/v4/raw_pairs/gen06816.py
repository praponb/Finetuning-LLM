from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").between(2.3, 2.6))
    .groupBy(
        F.col("p.category"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("category").desc(), "total_quantity")
)
