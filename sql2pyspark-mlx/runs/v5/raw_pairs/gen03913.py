from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("balance")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        "customer_id",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.percent_rank().over(w2).alias("balance_pct_rank"),
    )
    .orderBy(F.col("id").desc())
)
