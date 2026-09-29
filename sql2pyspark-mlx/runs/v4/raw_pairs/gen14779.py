from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("budget") <= 3975000) & (F.col("location") != "London"))
)

summary = (
    base
    .groupBy("dept_name")
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
)

result = (
    summary
    .filter(F.col("unique_dept_names") >= 3)
    .select("dept_name", "unique_dept_names")
    .orderBy(F.col("unique_dept_names").desc(), "dept_name")
    .limit(1)
)
