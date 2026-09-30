from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") > "2025-04-06") & (F.col("city") == "Chiang Mai"))
    .groupBy("city")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .filter(F.col("cnt") > 2)
    .orderBy("city", F.col("cnt").desc())
)
