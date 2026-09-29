from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy("balance", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "account_type",
        "balance",
        F.row_number().over(w).alias("rn"),
    )
    .orderBy("status", "account_type")
)
