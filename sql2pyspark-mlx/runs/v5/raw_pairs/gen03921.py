from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-02-08", "2025-07-01") | (F.col("revenue") >= 1300))
)

store_id_summary = (
    valid_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = store_id_summary
