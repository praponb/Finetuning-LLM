from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull() & F.col("product_name").isNotNull())
    .select(
        "id",
        "product_name",
        F.round((F.col("cost") * 1.15), 2).alias("cost_adjusted"),
    )
    .orderBy("id", F.col("cost_adjusted").desc())
)
