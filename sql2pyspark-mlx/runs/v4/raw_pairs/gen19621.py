from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") >= "2023-08-01") & (F.col("is_remote") | (F.col("salary") < 73000)))
    .select(
        "dept_id",
        "salary",
        "full_name",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy(F.col("salary").desc(), "full_name")
)
