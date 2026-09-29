from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)
