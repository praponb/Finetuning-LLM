from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name", "location")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum("budget").alias("total_budget"),
    )
)
