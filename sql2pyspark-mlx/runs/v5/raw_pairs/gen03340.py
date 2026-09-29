from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
    .orderBy("city_values", "city")
    .limit(3)
)
