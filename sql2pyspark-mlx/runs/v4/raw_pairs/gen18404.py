from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%ive%") & (F.col("sqft") <= 7861))
    .select(
        "sqft",
        "opened_date",
        "city",
        F.initcap(F.trim("city")).alias("city_title"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)
