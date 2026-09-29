from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("dept_id").isin([5, 11, 40]))

summary = (
    filtered_employees
    .groupBy(
        F.when(F.col("job_title").isin(["Manager", "Director"]), "core").otherwise("other").alias("job_title_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = summary.orderBy("level_values")
