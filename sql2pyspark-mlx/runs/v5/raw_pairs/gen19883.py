from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("amount") > 2400)
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "txn_time",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.row_number().over(w2).alias("row_num"),
    )
)
