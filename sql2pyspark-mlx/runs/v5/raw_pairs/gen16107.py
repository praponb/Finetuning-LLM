from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 29]))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
)
