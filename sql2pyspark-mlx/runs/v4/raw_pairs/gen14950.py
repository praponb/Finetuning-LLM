from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.concat_ws("-", "device", "country").alias("device_country_key"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

result = (
    base
    .groupBy("country")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy(F.col("row_count_2").asc_nulls_last())
)
