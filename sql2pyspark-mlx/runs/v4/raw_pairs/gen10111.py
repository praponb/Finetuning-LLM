from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc())
w2 = Window.partitionBy("manager_id")
w3 = Window.partitionBy("job_title")

scored = (
    df_employees
    .select(
        "*",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.max("salary").over(w3).alias("job_title_max_salary"),
    )
)

result = (
    scored
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "salary_dense_rank",
        "salary_share",
        "job_title_max_salary",
    )
)
