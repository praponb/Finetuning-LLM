from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy("balance")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "balance",
        F.row_number().over(w).alias("rn"),
        F.rank().over(w2).alias("balance_rank"),
    )
)
