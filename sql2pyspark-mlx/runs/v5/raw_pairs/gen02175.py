from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("opened_date") < "2025-04-01")
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "account_type", "balance", "opened_date")
)
