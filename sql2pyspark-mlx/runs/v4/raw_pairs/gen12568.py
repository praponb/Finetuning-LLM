from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("salary").between(104000, 133000))
    .select(
        "id",
        "hire_date",
        "salary",
        "dept_id",
        F.lead("salary", 1).over(w).alias("next_salary"),
        F.row_number().over(w2).alias("hire_date_row"),
    )
)
