from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "is_flagged",
        F.sum("amount").over(w).alias("last4_sum_amount"),
        F.avg("amount").over(w).alias("last4_avg_amount"),
        F.count("amount").over(w).alias("last4_count_amount"),
    )
)
