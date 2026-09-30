from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        (F.col("salary") - F.lag("salary", 1).over(w)).alias("salary_change"),
    )
)
