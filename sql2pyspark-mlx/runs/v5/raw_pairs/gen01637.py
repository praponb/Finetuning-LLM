from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("store_sales.revenue") < 2000)
    .groupBy(
        F.when(F.year(F.col("store_sales.sale_date")) < 2024, "before_2024").when(F.year(F.col("store_sales.sale_date")) == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
        F.min(F.col("inventory.last_restock")).alias("min_last_restock"),
    )
    .orderBy("sale_cohort")
)
