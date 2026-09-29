from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
