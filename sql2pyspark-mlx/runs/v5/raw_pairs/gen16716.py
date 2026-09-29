from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        F.lead("salary", 1).over(w).alias("next_salary"),
    )
    .orderBy("level")
)
