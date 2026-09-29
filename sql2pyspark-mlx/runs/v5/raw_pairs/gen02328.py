from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-12-22", "2024-08-01"))
    .select(
        "id",
        "dept_id",
        "salary",
        "is_remote",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "dept_id", "salary", "is_remote")
    .orderBy(F.col("dept_id").desc())
)
