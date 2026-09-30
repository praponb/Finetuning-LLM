from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 13)
    .groupBy(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 69000, "mid").otherwise("low").alias("salary_bucket"),
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sum("salary").alias("total_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
    )
)
