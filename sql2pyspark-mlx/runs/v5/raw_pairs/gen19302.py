from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

deduped = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "event_type", "page")
)

result = (
    deduped
    .rollup("page")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)
