from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_accounts
    .select(
        "*",
        F.sum("balance").over(w).alias("account_type_sum_balance"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("balance").over(w3).alias("cumulative_balance"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "account_type_sum_balance",
        "cumulative_balance",
        F.when(F.col("status").rlike("^(OPEN|CLOSED)$"), "EXTENDED").when(F.col("status").rlike("^(FROZEN)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
)
