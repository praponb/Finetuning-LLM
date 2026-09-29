from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_frozen", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("is_frozen", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type", "status").orderBy("balance", "id")

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-18").cast("date")) & (F.col("status") == "CLOSED"))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("status") == "FROZEN", 1).otherwise(0).alias("is_frozen"),
    )
)

result = (
    base
    .select(
        "*",
        F.avg("is_frozen").over(w).alias("is_frozen_avg_3"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.lead("is_frozen", 1, 0).over(w3).alias("next_is_frozen"),
    )
)
