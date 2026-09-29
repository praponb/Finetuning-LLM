from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 5, 6, 9]))
    .select(
        "id",
        "product_id",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
