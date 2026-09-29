from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        "account_type",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.row_number().over(w2).alias("opened_date_row"),
    )
    .orderBy("account_type", "id")
)
