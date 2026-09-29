from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([4, 7, 9]) & (F.lower(F.trim("store_name")) == "airport kiosk") & (F.col("opened_date") <= "2024-12-16"))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("total_sqft").desc(), F.col("opened_month").desc())
    .limit(1)
)
