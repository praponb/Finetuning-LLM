from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance")

result = df_accounts.select("id", "balance", F.dense_rank().over(w).alias("dense_rnk"))
