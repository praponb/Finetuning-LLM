from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 77000) & (F.col("hire_date") <= "2025-06-01"))
    .groupBy(
        "is_remote",
        F.when(F.col("salary") >= 164000, "high").when(F.col("salary") >= 58000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
    )
    .filter(F.col("pct_l2") >= 77)
)
