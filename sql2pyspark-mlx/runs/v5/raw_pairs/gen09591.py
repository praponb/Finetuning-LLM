from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Sales", "HR"]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .orderBy("dept_name")
)
