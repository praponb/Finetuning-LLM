from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 20, 32]))
    .select("id", "account_id", "amount", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "account_id", "amount")
    .orderBy(F.col("amount").desc())
)
