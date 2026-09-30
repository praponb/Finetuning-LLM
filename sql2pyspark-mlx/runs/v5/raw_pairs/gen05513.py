from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("salary").between(81000, 140000))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "dept_id", "hire_date")
    .orderBy("id")
)
