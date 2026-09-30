from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "Central") & F.col("store_name").like("%Old%"))
    .groupBy("region")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
