from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("salary", "id")
w2 = Window.orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.percent_rank().over(w3).alias("salary_percent_rank"),
    )
)
