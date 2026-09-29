from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .filter(F.col("budget").between(1953000, 3520000))
    .select("id", "location", "budget", F.rank().over(w).alias("rnk"))
)
