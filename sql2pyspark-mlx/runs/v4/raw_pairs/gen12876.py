from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-08-01")
    .select(
        "id",
        "product_id",
        "units",
        F.year("sale_date").alias("sale_date_year"),
        F.when(F.col("units") >= 44, "High").when(F.col("units") >= 20, "Medium").otherwise("Low").alias("units_band"),
    )
    .orderBy(F.col("product_id").desc())
)
