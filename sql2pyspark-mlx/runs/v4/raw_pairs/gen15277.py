from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.budget") >= 3757000)
    .groupBy(
        F.col("d.dept_name"),
        F.when(F.col("d.location").isin(["Chiang Mai", "London"]), "core").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
    .orderBy("location_group")
)
