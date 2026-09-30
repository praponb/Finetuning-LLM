from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter((F.col("city") == "Berlin") & F.col("opened_date").between("2023-04-01", "2024-05-25"))
)

stores_stats = (
    filtered_stores
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
)

result = (
    stores_stats
    .filter(F.col("unique_regions") < 8)
    .select("city", "opened_month", "city_values", "unique_regions")
    .orderBy("opened_month")
)
