from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("product_id") == 7) & F.col("product_id").isin([5, 6, 8, 10]) & F.col("last_restock").between("2023-03-19", "2025-07-01"))
)

product_id_summary = (
    recent_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 261, "high").when(F.col("quantity") >= 66, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    product_id_summary
    .select("product_id", "quantity_bucket", "max_quantity")
    .orderBy("quantity_bucket")
)
