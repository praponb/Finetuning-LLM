from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.dense_rank().over(w2).alias("opened_date_dense_rank"),
    )
)
