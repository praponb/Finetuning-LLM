from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit")

result = (
    df_customers
    .filter(F.col("city") == "Chiang Mai")
    .select("id", "country", "credit_limit", "age", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "country", "credit_limit", "age")
    .orderBy(F.col("id").desc())
)
