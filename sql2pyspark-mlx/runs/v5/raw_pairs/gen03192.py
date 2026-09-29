from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select("id", "customer_id", "balance", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "customer_id", "balance")
    .orderBy(F.col("customer_id").desc())
)
