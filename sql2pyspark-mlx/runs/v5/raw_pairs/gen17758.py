from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "hire_date",
        "job_title",
        "salary",
        F.row_number().over(w).alias("rn"),
        F.lead("salary", 2).over(w2).alias("next_salary"),
    )
    .orderBy("job_title")
)
