from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("status") != "OPEN")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "account_type", "balance", "status")
)
