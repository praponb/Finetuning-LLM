from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("balance", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").desc(), "id")
w_running = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.lead("balance", 1).over(w_hist).alias("next_balance"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)
