from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "customer_id", "opened_date", "balance")
    .orderBy("customer_id", F.col("id").desc())
)
