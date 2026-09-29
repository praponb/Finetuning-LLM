from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "segment", "credit_limit")
    .orderBy(F.col("id").desc())
)
