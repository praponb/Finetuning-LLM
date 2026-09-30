from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "dept_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 5)
    .select("id", "manager_id", "hire_date", "dept_id")
    .orderBy(F.col("hire_date").desc())
)
