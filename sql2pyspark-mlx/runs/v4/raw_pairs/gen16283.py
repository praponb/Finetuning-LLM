from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w2 = Window.partitionBy("dept_id")
w3 = Window.partitionBy("job_title")

with_windows = (
    df_employees
    .select(
        "*",
        F.ntile(3).over(w).alias("salary_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.max("salary").over(w3).alias("job_title_max_salary"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "salary_bucket",
        "job_title_max_salary",
        F.when(F.col("full_name").rlike("^(Pim Chaiyaporn|Ivy Chen)$"), "EXTENDED").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "CORE").otherwise("GROUP_A").alias("full_name_zone"),
    )
    .orderBy(F.col("job_title_max_salary").desc_nulls_first(), "job_title")
)
