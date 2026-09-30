from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy("balance", "id")
w2 = Window.orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("balance") <= 25000)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.ntile(10).over(w).alias("balance_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
