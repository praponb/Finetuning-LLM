from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(~F.col("level").isin(["L3", "L4", "L1"]))
    .select("id", "job_title", "hire_date", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "job_title", "hire_date")
    .orderBy(F.col("hire_date").desc())
)
