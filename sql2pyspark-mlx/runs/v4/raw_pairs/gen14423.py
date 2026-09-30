from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 124000, "high").when(F.col("salary") >= 80000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 356000)
)
