from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("s.country") == "JP")
    .groupBy(
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
        F.year(F.col("p.launch_date")).alias("launch_year"),
    )
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
    )
    .orderBy("launch_year", F.col("launch_quarter").desc())
)
