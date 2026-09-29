from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "balance",
        "account_type",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy("status")
)
