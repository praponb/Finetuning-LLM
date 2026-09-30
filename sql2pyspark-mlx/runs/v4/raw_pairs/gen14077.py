from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"), "store_id")
    .agg(
        F.count("units").alias("units_count"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("units_count") < 9)
    .select("sale_year", "store_id", "units_count", "units_range")
)
