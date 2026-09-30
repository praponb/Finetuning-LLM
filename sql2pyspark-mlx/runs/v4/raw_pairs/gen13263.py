from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 18)
    .agg(
        F.sum("revenue").alias("total_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("units_range")
)
