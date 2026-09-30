from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("product_id").desc())
)
