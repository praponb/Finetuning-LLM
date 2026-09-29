from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select("id", "location", "budget", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "location", "budget")
    .orderBy(F.col("budget").desc())
)
