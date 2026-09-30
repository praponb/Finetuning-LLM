from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 10)
    .groupBy("is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)
