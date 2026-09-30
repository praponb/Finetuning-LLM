from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1479000, 3283000))
    .groupBy(
        "dept_name",
        F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") < 3308000)
    .orderBy(F.col("avg_budget").desc(), "dept_name", "dept_name_group")
    .limit(5)
)
