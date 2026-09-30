from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .select("id", "dept_id", "hire_date", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("hire_date").desc())
)
