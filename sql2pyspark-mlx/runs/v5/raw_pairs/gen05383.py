from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("budget") <= 3903000)

agg = (
    filtered_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3061000, "high").when(F.col("budget") >= 1441000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)

result = agg.orderBy("budget_bucket")
