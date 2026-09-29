from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)
