from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
)
