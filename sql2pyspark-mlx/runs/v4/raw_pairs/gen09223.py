from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        F.dense_rank().over(w).alias("amount_dense_rank"),
    )
    .orderBy(F.col("amount").desc())
)
