from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("quantity").between(143, 188))

product_id_summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    product_id_summary
    .select("product_id", "unique_products", "max_quantity")
    .orderBy(F.col("max_quantity").desc(), "product_id")
    .limit(3)
)
