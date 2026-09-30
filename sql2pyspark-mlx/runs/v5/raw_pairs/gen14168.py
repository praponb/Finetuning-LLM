from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("salary").between(83000, 123000))
    .groupBy("is_remote", "job_title")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.min("salary").alias("min_salary"),
    )
)
