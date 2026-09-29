from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")

result = (
    df_employees
    .filter(F.col("job_title") == "Engineer")
    .select("id", "level", "salary", F.min("salary").over(w).alias("level_min_salary"))
)
