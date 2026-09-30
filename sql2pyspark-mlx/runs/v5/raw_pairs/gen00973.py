from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "salary",
        "dept_id",
        (F.col("salary") - F.lag("salary", 2).over(w)).alias("salary_change"),
        F.max("salary").over(w2).alias("dept_id_max_salary"),
    )
)
