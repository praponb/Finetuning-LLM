from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.concat_ws("-", "device", "country").alias("device_country_key"),
    )
)

result = (
    cleaned
    .groupBy("customer_id", "country")
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy("country")
)
