from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .select("id", "level", "hire_date", "email", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "level", "hire_date", "email")
    .orderBy("hire_date")
)
