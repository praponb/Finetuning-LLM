from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date")

result = (
    df_employees
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
)
