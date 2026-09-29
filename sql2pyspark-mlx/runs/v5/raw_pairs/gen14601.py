from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper(F.trim("email")) == "ALICE@GMAIL.COM")
    .groupBy(
        F.when(F.col("salary") >= 164000, "high").when(F.col("salary") >= 52000, "mid").otherwise("low").alias("salary_bucket"),
        "dept_id",
    )
    .agg(
        F.min("salary").alias("min_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .filter(F.col("min_salary") > 104000)
    .orderBy(F.col("min_salary").desc())
)
