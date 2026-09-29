from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.orderBy("opened_date")

result = (
    df_accounts
    .filter(F.col("balance") >= 75000)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("balance_row"),
        F.dense_rank().over(w2).alias("opened_date_dense_rank"),
    )
)
