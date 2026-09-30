from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("browser")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
)
