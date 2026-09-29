from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.sale_date").between("2023-12-01", "2025-05-01"))
    .groupBy(
        F.when(F.col("ss.revenue") >= 3500, "high").when(F.col("ss.revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .orderBy(F.col("min_quantity").desc())
)
