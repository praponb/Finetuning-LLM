from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_employees
    .filter(F.col("manager_id").isin([31, 35, 40]))
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_7"),
    )
)
