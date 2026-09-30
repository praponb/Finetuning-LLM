from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit")

result = (
    df_customers
    .select("id", "city", "credit_limit", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "city", "credit_limit")
    .orderBy("id")
)
