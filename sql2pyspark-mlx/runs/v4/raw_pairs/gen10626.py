from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        F.avg("amount").over(w).alias("account_id_avg_amount"),
    )
)
