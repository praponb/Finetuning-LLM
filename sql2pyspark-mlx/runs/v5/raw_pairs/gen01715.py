from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .filter(F.col("balance").between(20000, 68000))
    .select(
        "id",
        "opened_date",
        "status",
        F.row_number().over(w).alias("row_seq"),
        F.last("balance", ignorenulls=True).over(w2).alias("last_known_balance"),
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status"),
    )
)
