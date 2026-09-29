from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") >= 8108) & F.col("store_name").like("%Cen%") & ~F.col("city").isin(["Singapore", "New York", "London"]))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.max("opened_date").alias("latest_opened_date"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("unique_regions") > 5)
)
