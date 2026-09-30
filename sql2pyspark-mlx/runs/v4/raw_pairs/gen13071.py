from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")

result = (
    df_departments
    .select("id", "dept_name", F.count("*").over(w).alias("dept_name_count"))
)
