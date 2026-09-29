from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
