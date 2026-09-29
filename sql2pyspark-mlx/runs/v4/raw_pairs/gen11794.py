from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(12, 21) | F.col("units").between(21, 26))
    .select(
        "id",
        "store_id",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
