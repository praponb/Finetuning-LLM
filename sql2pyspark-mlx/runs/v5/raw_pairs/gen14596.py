from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "status",
        F.sum("balance").over(w_part).alias("last7_sum_balance"),
        F.min("balance").over(w_part).alias("last7_min_balance"),
        F.avg("balance").over(w_part).alias("last7_avg_balance"),
        F.count("balance").over(w_part).alias("last7_count_balance"),
    )
)
