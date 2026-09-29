from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.when(F.col("page").isin(["/home", "/product"]), "group_a").otherwise("other").alias("page_group"))
    .pivot("event_type", ["click", "search"])
    .agg(F.sum("duration_sec"))
    .orderBy("page_group")
)
