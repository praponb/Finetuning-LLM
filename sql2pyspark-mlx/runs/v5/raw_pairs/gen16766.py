from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 13))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.first("amount").over(w2).alias("first_amount"),
    )
)
