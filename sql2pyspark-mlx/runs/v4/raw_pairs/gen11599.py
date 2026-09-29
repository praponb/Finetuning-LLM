from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select(
        "*",
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.rank().over(w).alias("balance_rank"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
    )
)
