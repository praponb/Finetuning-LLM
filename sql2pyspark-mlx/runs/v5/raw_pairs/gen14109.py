from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("london_count") > 2)
)
