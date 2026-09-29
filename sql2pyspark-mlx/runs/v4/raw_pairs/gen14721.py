from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "*",
        F.count("amount").over(w).alias("amount_count_90d"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
