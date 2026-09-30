from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") < 1400) & F.col("units").between(25, 34) & (F.col("product_id") == 6))
    .select(
        "id",
        "product_id",
        F.when(F.col("units") >= 31, "High").when(F.col("units") >= 8, "Medium").otherwise("Low").alias("units_tier"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)
