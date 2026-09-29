from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "customer_id",
        "balance",
        F.first("customer_id").over(w).alias("first_customer_id"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
    )
)
