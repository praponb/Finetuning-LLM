from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("account_id").isin([3, 12, 31]))
    .select("id", "account_id", "amount", "is_flagged", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .orderBy(F.col("id").desc(), F.col("rnk").desc())
)
