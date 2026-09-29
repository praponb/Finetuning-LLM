from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "level",
        "salary",
        "hire_date",
        F.first("hire_date").over(w).alias("first_hire_date"),
    )
    .orderBy("hire_date")
)
