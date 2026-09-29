from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("e.dept_id") == 10)
    .groupBy(
        F.col("d.location"),
        F.when(F.col("d.dept_name").isin(["Engineering", "Legal"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"))
    .orderBy(F.col("location").desc(), "dept_name_group")
)
