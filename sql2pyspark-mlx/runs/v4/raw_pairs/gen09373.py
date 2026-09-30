from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("account_id").isin([21, 22, 32]))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        F.last("merchant").over(w).alias("last_merchant"),
    )
)
