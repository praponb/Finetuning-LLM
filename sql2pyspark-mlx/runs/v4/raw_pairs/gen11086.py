from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-10-08")))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        F.upper("level").alias("level_upper"),
    )
)

result = (
    base
    .select(
        "*",
        F.min("salary").over(w).alias("manager_id_min_salary"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
        F.sum("salary").over(w3).alias("running_salary"),
    )
)
