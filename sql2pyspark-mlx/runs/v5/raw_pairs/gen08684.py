from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("manager_id") == 22)
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
    )
    .orderBy(F.col("id").desc())
)
