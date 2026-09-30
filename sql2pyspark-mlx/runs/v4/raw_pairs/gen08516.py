from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
    .orderBy(F.col("page").desc())
)
