from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "email",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
)
