from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([24, 35, 36, 39]))
    .select(
        "id",
        "merchant",
        "txn_time",
        "account_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "merchant", "txn_time", "account_id")
    .orderBy("merchant")
)
