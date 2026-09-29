from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Tokyo"]))
    .select("id", "city", "rating", F.trim("city").alias("clean_city"))
)

result = (
    cleaned
    .groupBy("city")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy(F.col("city").desc())
)
