from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("salary") > 114000)
    .select(
        "id",
        "job_title",
        "dept_id",
        "hire_date",
        "salary",
        "manager_id",
        "level",
        (F.col("salary") - F.lag("salary", 1).over(w)).alias("salary_change"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy("manager_id")
)
