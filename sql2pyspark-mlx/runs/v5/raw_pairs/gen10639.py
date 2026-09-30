from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Consumer"]))
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 1)
)
