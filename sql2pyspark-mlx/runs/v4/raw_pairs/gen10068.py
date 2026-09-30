from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
    )
    .orderBy("location_values", "marketing_count")
)
