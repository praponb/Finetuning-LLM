from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 20) & F.month("sale_date").isin([3, 8, 12]))
    .select(
        "store_id",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)
