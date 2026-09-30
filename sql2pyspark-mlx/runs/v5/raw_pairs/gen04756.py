from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

result = (
    df_employees
    .select("id", "manager_id", "salary", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("salary").desc())
)
