from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-10-14", "2025-09-17") & (F.col("salary") > 57000))
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
    )
    .filter(F.col("manager_count") >= 77)
    .orderBy("dept_id")
)
