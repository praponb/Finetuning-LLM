from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
