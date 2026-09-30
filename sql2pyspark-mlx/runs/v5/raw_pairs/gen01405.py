from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.hour("txn_time").between(9, 17))
    .select(
        "id",
        "account_id",
        "txn_time",
        "txn_type",
        "amount",
        F.row_number().over(w).alias("rn"),
        F.ntile(2).over(w2).alias("quartile"),
    )
    .orderBy(F.col("quartile").desc(), F.col("rn").desc())
)
