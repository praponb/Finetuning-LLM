from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "dept_id", "hire_date", "full_name")
)
