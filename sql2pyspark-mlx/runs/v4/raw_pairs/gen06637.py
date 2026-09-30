from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("hire_date", "id")
w_grp = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("is_remote"))
    .select("*", F.trim("level").alias("clean_level"))
)

result = (
    cleaned
    .select(
        "*",
        F.row_number().over(w_part).alias("event_seq"),
        F.sum("salary").over(w_grp).alias("running_salary"),
    )
)
