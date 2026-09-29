from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("balance") <= 57000)
    .select(
        "id",
        "account_type",
        "status",
        "balance",
        F.row_number().over(w).alias("rn"),
    )
)
