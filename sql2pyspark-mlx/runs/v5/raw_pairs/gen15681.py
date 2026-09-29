from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "CREDIT"]))
    .select(
        "*",
        F.sum("amount").over(w_running).alias("running_sum_amount"),
        F.max("amount").over(w_running).alias("running_max_amount"),
        F.min("amount").over(w_running).alias("running_min_amount"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)
