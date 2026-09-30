from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")

result = (
    df_transactions
    .select("id", "txn_type", "amount", "merchant", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "txn_type", "amount", "merchant")
)
