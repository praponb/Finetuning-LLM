from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.product_id") == 8)
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), F.col("supplier_id").desc())
)
