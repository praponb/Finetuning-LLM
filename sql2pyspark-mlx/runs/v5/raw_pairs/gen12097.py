from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("amount") <= 1300)
    .select("id", "txn_type", "amount", F.ntile(4).over(w).alias("quartile"))
)
