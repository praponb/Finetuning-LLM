from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("salary").desc())
)
