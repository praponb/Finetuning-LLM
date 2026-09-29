from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("location") != "Singapore") | (F.col("dept_name") == "Marketing"))
    .withColumn("location_upper", F.upper("location"))
)

summary = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
)

result = summary
