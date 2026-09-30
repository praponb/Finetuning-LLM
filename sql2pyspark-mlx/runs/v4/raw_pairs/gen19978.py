from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("amount") < 1600)
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
)
