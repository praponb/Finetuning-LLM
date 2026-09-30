from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit_2"),
    )
)
