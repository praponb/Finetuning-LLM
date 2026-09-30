from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.lower(F.col("suppliers.supplier_name")).like("%oky%"))
    .groupBy(F.col("suppliers.country"))
    .agg(
        F.countDistinct(F.col("inventory.quantity")).alias("unique_products"),
        F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("avg_quantity", F.col("unique_products").desc())
)
