from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("account_type")

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "LOAN", "CHECKING"]))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.avg("balance").over(w).alias("balance_avg_5"),
        F.sum("balance").over(w2).alias("account_type_sum_balance"),
    )
)
