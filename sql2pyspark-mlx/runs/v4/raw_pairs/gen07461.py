from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2024-02-04").cast("date"))
    .select("id", "city", F.date_sub("opened_date", 75).alias("opened_date_minus_75d"))
)

result = (
    base
    .withColumn("week_start", F.date_trunc("week", "opened_date_minus_75d"))
    .rollup("week_start")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .orderBy(F.col("num_cities").desc())
)
