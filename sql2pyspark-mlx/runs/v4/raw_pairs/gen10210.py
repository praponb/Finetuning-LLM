from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        "account_type",
        F.lead("balance", 1, 0).over(w).alias("next_balance"),
        F.rank().over(w2).alias("balance_rank"),
    )
)
