from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy(F.col("started_at").asc_nulls_first(), "id")

latest = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("last_seen"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("num_browsers").desc_nulls_first())
)
