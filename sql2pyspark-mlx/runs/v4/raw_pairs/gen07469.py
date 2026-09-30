from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "account_type").orderBy("opened_date_plus_62d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("balance", "id")

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-07-20"))) & (F.col("balance") > 25000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_add("opened_date", 62).alias("opened_date_plus_62d"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

result = (
    valid
    .select(
        "*",
        F.last("status", ignorenulls=True).over(w).alias("last_known_status"),
        F.first("balance", ignorenulls=True).over(w2).alias("first_known_balance"),
        F.lag("balance", 1).over(w3).alias("prev_balance"),
    )
)
