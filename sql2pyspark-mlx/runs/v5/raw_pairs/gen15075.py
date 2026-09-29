from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-10-06").cast("date"))
    .select(
        "*",
        F.when(F.col("units") >= 45, "High").when(F.col("units") >= 8, "Medium").otherwise("Low").alias("units_band"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.units")).alias("min_units"),
    )
)
