from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("txn_time") < "2023-03-02")
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "txn_type", "txn_time", "amount")
    .orderBy("amount")
)
