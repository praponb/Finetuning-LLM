from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("balance", "id")

scored = (
    df_accounts
    .select(
        "*",
        F.sum("balance").over(w).alias("cumulative_balance"),
        F.lead("balance", 2).over(w2).alias("next_balance"),
        F.ntile(10).over(w2).alias("balance_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "cumulative_balance",
        "next_balance",
        "balance_bucket",
        "seq_no_2",
    )
    .orderBy("status")
)
