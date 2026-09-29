from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("product_id") == 7) & F.col("product_id").isin([1, 3])) | (F.col("product_id") == 4))
    .select(
        "id",
        "store_id",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc())
)
