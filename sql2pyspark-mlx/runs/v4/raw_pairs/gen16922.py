from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("central_count").desc())

base = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-09-24").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

result = (
    base
    .groupBy("city", "region")
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .withColumn("central_count_percent_rank", F.percent_rank().over(w))
)
