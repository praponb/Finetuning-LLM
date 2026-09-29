from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 60)
    .groupBy("segment")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
    .orderBy("segment")
)
