from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4, 5]) & (F.col("sale_date") > "2025-05-01") & F.col("store_id").isin([1, 2, 4]))
    .select(
        "id",
        "store_id",
        "units",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
