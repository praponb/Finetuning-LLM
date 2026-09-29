from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Jac%") & F.col("dept_id").isin([4, 10, 11, 15]))
    .agg(
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)
