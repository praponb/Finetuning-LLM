from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")
w2 = Window.partitionBy("status").orderBy("balance")

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS", "CHECKING"]))
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        F.percent_rank().over(w).alias("opened_date_pct_rank"),
        F.percent_rank().over(w2).alias("balance_pct_rank"),
    )
)
