from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("opened_date", "id")

scored = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_2"),
        F.first("balance", ignorenulls=True).over(w2).alias("first_known_balance"),
        F.avg("balance").over(w3).alias("balance_avg_2_prior"),
        F.lag("balance", 2, 0).over(w4).alias("prev_balance"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "balance_avg_2",
        "first_known_balance",
        "prev_balance",
    )
    .orderBy(F.col("id").desc())
)
