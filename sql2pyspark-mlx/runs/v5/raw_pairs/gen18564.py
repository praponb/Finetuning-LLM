from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("salary")

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_3"),
        F.rank().over(w2).alias("rnk"),
    )
)
