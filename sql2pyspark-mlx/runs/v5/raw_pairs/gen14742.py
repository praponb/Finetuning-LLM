from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "manager_id",
        F.when(F.col("salary") >= 154000, "high").when(F.col("salary") >= 55000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
        F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)
