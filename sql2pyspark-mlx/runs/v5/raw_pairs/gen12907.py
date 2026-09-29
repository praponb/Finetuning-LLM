from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.orderBy(F.col("txn_time").desc(), "id")
w_ordered = Window.partitionBy("txn_type", "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

scored = (
    df_transactions
    .select(
        "*",
        F.row_number().over(w_recent).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.count("amount").over(w_ordered).alias("amount_count_90d"),
    )
)

result = scored.select("id", "amount", "row_seq", "amount_count_90d")
