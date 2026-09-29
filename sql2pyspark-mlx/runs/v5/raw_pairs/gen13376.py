from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("manager_id").isin([5, 13]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy("hire_date")
)
