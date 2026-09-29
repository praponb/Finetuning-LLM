from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time_quarter", "id")
w2 = Window.partitionBy("account_id", "merchant").orderBy("amount", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("account_id").isin([18, 34]))
    .withColumn("merchant_upper", F.upper("merchant"))
    .withColumn("txn_time_quarter", F.quarter("txn_time"))
)

result = (
    cleaned
    .select(
        "*",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.lag("txn_time_quarter", 2).over(w2).alias("prev_txn_time_quarter"),
        F.avg("txn_time_quarter").over(w3).alias("txn_time_quarter_avg_90d"),
    )
)
