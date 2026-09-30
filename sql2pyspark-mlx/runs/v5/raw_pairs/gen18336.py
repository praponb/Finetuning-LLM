from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 1)
    .orderBy("hire_date", "id")
)
