from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "New York"]))
    .select("id", "location", "budget", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("id").desc(), "budget")
)
