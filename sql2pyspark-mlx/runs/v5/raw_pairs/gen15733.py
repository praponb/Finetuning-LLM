from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").asc_nulls_first(), "id")

ranked = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.countDistinct("country").alias("distinct_countries"),
        F.size(F.collect_set("country")).alias("num_countries_2"),
    )
    .orderBy(F.col("num_devices").desc_nulls_first())
)
