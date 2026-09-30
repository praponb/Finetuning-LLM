from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.date_format(F.col("i.last_restock"), "yyyy-MM-dd").alias("last_restock_label"),
    )
    .orderBy(F.col("last_restock_label").desc(), "launch_date")
)
