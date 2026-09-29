from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Manager") & (F.col("manager_id") == 25))
    .groupBy("job_title")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"))
    .orderBy("level_values")
)
