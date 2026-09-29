from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Marketing"]))
    .select("id", "location", "budget", F.rank().over(w).alias("rnk"))
)
