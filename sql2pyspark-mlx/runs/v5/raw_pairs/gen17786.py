from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id")

result = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
