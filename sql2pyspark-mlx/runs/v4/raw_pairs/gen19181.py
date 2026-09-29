from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .filter(F.col("city").isin(["Berlin", "Bangkok"]))
    .select("id", "city", "credit_limit", "age", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("age").desc(), F.col("city").desc())
)
