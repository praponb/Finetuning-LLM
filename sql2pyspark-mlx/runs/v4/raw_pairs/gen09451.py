from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("manager_id", "level")

result = (
    df_employees
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)
