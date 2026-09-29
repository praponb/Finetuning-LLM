from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")

result = (
    df_departments
    .filter(F.col("location") == "London")
    .select("id", "location", "dept_name", F.count("*").over(w).alias("location_count"))
)
