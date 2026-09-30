from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_4"),
        F.percent_rank().over(w2).alias("salary_pct_rank"),
    )
)
