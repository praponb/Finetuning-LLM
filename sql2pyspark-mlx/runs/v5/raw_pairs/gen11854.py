from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "dept_id",
        "level",
        "hire_date",
        "salary",
        (F.col("salary") - F.lag("salary", 1, 0).over(w)).alias("salary_change"),
    )
)
