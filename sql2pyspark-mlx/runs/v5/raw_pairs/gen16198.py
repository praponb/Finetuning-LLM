from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w_grp = Window.orderBy("next_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("seq_no").desc())
w4 = Window.partitionBy("status").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("opened_date", "id")
w_ordered = Window.partitionBy("status").orderBy(F.col("next_balance").desc_nulls_last())

valid = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select("*", F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") > 1500), "left_anti")
)

with_windows = (
    not_flagged
    .filter(F.col("account_type") != "SAVINGS")
    .select(
        "id",
        "opened_date",
        "status",
        "opened_date_label",
        F.lead("balance").over(w_part).alias("next_balance"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "opened_date_label",
        "next_balance",
        "seq_no",
        F.first("next_balance", ignorenulls=True).over(w_grp).alias("first_known_next_balance"),
        F.rank().over(w3).alias("seq_no_rank"),
        F.first("next_balance", ignorenulls=True).over(w4).alias("first_known_next_balance_2"),
        F.lead("next_balance", 2).over(w5).alias("next_next_balance"),
        F.rank().over(w_ordered).alias("next_balance_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "next_balance",
        "first_known_next_balance",
        "seq_no_rank",
        "first_known_next_balance_2",
    )
)
