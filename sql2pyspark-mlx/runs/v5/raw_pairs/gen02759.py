from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("opened_date") < "2023-11-01")
    .select(
        "id",
        "customer_id",
        "account_type",
        F.lag("balance", 1, 0).over(w).alias("prev_balance"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
