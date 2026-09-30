from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

ranked = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("duration_sec") >= 391, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.countDistinct("page").alias("distinct_pages"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
    .orderBy(F.col("duration_sec_tier").asc_nulls_first())
)
