from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit")

result = (
    df_customers
    .filter(F.col("age").between(32, 39))
    .select("id", "segment", "credit_limit", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "segment", "credit_limit")
)
