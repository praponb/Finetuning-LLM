from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").asc_nulls_first(), "id")

ranked = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.max("started_at").alias("last_seen"),
    )
)
