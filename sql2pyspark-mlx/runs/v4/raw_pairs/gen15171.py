from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.partitionBy("customer_id")

result = (
    df_accounts
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w).alias("balance_bucket"),
        F.min("balance").over(w2).alias("customer_id_min_balance"),
    )
)
