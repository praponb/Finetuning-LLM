from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id")

result = (
    df_accounts
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.min("balance").over(w2).alias("customer_id_min_balance"),
    )
)
