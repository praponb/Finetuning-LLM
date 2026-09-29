from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_employees
    .filter(F.col("salary").between(57000, 77000))
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_4"),
    )
    .orderBy("salary")
)
