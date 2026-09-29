from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 4) & (F.col("revenue") <= 2300))
    .select(
        "id",
        "store_id",
        "units",
        "sale_date",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(5)
)
