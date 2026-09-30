from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        "status",
        "customer_id",
        F.lag("balance", 1).over(w).alias("prev_balance"),
        F.lag("opened_date", 1).over(w2).alias("prev_opened_date"),
    )
    .orderBy("opened_date")
)
