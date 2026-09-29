from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .filter(F.col("customer_id") == 14)
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "status", "opened_date", "balance")
    .orderBy("id", "balance")
)
