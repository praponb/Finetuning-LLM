from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") <= 2700) & F.month("sale_date").isin([1, 6, 11])) | F.col("store_id").isin([1, 3]))
    .select(
        "id",
        "sale_date",
        "store_id",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit_2"),
    )
)
