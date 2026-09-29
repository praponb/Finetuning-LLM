from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")

result = (
    df_employees
    .filter(F.col("hire_date") < "2023-08-01")
    .select("id", "manager_id", F.count("*").over(w).alias("manager_id_count"))
    .orderBy(F.col("manager_id").desc(), "manager_id_count")
)
