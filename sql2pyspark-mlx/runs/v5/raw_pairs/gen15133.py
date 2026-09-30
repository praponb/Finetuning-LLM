from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .groupBy(
        F.col("products.supplier_id"),
        F.when(F.year(F.col("products.launch_date")) < 2024, "before_2024").when(F.year(F.col("products.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("total_quantity").desc(), "supplier_id", "launch_cohort")
    .limit(5)
)
