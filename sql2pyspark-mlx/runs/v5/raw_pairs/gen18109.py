from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .select("id", "location", F.count("*").over(w).alias("location_count"))
    .orderBy(F.col("id").desc(), "location")
)
