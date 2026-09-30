from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-06-28").cast("date"))
    .select("*", F.round((F.col("salary") * 100), 2).alias("salary_adjusted"))
)

result = (
    valid
    .select("id", "dept_id", "salary_adjusted", "salary")
    .unpivot(["id", "dept_id"], ["salary_adjusted", "salary"], "kpi", "metric_value")
    .filter(F.col("metric_value").isNotNull())
)
