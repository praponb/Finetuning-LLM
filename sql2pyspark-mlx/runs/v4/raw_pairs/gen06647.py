from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())
w_running = Window.partitionBy("account_id").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "*",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.lead("amount", 2).over(w_running).alias("next_amount"),
    )
)
