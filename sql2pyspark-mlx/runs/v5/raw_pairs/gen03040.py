from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_id", "level").orderBy("salary")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("level", "dept_id").orderBy("hire_date", "id").rowsBetween(-2, -1)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.dense_rank().over(w_part).alias("salary_dense_rank"),
        F.rank().over(w2).alias("salary_rank"),
        F.avg("salary").over(w3).alias("salary_avg_2_prior"),
    )
)
