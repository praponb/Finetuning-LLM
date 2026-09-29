from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_7"),
        (F.col("salary") - F.lag("salary", 1, 0).over(w2)).alias("salary_change"),
    )
    .orderBy(F.col("hire_date").desc())
)
