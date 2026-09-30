from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.lag("balance").over(w_ordered).alias("prev_balance"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.avg("balance").over(w2).alias("balance_avg_5"),
    )
)
