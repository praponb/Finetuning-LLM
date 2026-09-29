from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select("id", "txn_type", "amount", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "txn_type", "amount")
    .orderBy(F.col("id").desc(), "amount")
)
