from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("balance").between(21000, 24000))
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "account_type", "balance", "status")
)
