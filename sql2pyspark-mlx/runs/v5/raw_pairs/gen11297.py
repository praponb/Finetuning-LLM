from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_last(), "id")

deduped = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupingSets([["browser", "country"], ["browser"]], "browser", "country")
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.max("started_at").alias("ended_at"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("edge_events").asc_nulls_last())
)
