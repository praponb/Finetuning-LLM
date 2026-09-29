from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("job_title") == "Designer")
    .select("id", "manager_id", "hire_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "manager_id", "hire_date")
)
