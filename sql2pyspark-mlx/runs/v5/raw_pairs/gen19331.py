from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 1) & (F.col("revenue") >= 2300))
    .select(
        "sale_date",
        F.when(F.col("units") >= 36, "High").when(F.col("units") >= 16, "Medium").otherwise("Low").alias("units_band"),
        F.floor("revenue").alias("revenue_whole"),
    )
    .orderBy("units_band")
)
