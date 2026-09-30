from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "amount",
        "txn_type",
        "txn_time",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.last("amount").over(w2).alias("last_amount"),
    )
)
