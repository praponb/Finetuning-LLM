from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("opened_date") < "2025-08-07") & (F.col("region") == "Central")))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "region")
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
    .orderBy("opened_quarter")
)
