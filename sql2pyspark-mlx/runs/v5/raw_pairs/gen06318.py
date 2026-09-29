from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "segment", "credit_limit")
    .orderBy(F.col("id").desc(), "credit_limit")
)
