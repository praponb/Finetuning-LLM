from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select("id", "manager_id", "salary", "hire_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("id").desc())
)
