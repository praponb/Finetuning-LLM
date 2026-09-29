from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-10-01", "2025-09-01") | (F.col("sqft") <= 4979))
    .select(
        "opened_date",
        "city",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.concat_ws("/", "city", "region").alias("city_region_key"),
    )
    .orderBy("city_region_key")
)
