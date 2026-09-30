from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("customer_id", "device", "country")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
