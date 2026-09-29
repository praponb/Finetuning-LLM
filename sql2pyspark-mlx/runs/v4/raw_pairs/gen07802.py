from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 21) & (F.col("revenue") < 2000))
    .select(
        "id",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)
