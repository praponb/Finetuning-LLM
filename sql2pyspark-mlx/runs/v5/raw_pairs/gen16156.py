from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "status",
        F.min("balance").over(w).alias("running_min_balance"),
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.count("balance").over(w).alias("running_count_balance"),
        F.sum("balance").over(w).alias("running_sum_balance"),
    )
)
