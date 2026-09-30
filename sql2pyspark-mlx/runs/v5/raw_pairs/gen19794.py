from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") >= 3400) & (F.col("revenue") >= 1700)) | (F.col("sale_date") <= "2023-09-01"))
    .select(
        "store_id",
        "product_id",
        "units",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
        F.when(F.col("units") >= 36, "High").when(F.col("units") >= 12, "Medium").otherwise("Low").alias("units_band"),
    )
    .orderBy(F.col("units_band").desc())
)
