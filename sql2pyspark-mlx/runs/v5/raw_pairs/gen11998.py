from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select("id", "dept_id", "salary", "level", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "dept_id", "salary", "level")
)
