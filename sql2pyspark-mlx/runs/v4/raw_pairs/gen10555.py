from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("id", "manager_id", "hire_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "manager_id", "hire_date")
    .orderBy("manager_id")
)
