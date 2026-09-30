from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 139000, "high").when(F.col("salary") >= 49000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.max("hire_date").alias("last_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .orderBy("salary_bucket")
    .limit(5)
)
