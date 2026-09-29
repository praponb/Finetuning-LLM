from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "job_title",
        F.min("salary").over(w).alias("dept_id_min_salary"),
        F.row_number().over(w2).alias("rn"),
    )
)
