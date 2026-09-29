from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance")

result = (
    df_accounts
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "balance", "opened_date")
)
