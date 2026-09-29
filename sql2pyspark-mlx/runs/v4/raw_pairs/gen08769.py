from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L5", "L2"]))
    .select(
        "id",
        "level",
        "hire_date",
        "manager_id",
        "salary",
        F.rank().over(w).alias("hire_date_rank"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
