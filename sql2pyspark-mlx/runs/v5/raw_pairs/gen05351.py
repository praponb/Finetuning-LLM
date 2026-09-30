from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 11, 23, 34]))
    .select("id", "dept_id", "salary", F.row_number().over(w).alias("salary_row"))
    .orderBy(F.col("dept_id").desc(), F.col("salary").desc())
)
