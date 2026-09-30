from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.sort_array(F.collect_set("location")).alias("location_list"))
    .orderBy(F.col("location").desc())
)
