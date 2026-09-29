from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 1)
    .select("id", "account_id", "amount", "merchant")
    .orderBy("merchant")
)
