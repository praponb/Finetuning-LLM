from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance_rounded", "id")
w2 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy(F.col("balance_rounded").desc_nulls_last())
w5 = Window.partitionBy("account_type", "status").orderBy("balance_rounded", "id")

filtered = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.round("balance", 1).alias("balance_rounded"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("balance_rounded", 1, 0).over(w).alias("prev_balance_rounded"),
        F.last("balance", ignorenulls=True).over(w2).alias("last_known_balance"),
        F.first("balance", ignorenulls=True).over(w_ordered).alias("first_known_balance"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows_accounts = (
    deduped
    .withColumn("next_first_known_balance", F.lead("first_known_balance").over(w5))
    .withColumn("prev_first_known_balance", F.lag("first_known_balance").over(w5))
    .withColumn("next_balance", F.lead("balance").over(w5))
    .withColumn("prev_balance", F.lag("balance").over(w5))
    .withColumn("event_seq", F.row_number().over(w5))
)

result = (
    with_windows_accounts
    .select(
        "id",
        "status",
        F.col("prev_balance_rounded").alias("PREV_BALANCE_ROUNDED"),
        "prev_first_known_balance",
    )
    .unpivot(
        ids=["id", "status"],
        values=["PREV_BALANCE_ROUNDED", "prev_first_known_balance"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
