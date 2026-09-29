from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "manager_id", "hire_date")
    .orderBy(F.col("manager_id").desc())
)
