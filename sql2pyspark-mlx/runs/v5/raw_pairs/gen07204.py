from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .filter(F.month("opened_date").isin([6, 7, 10]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("seq_no"),
        F.last("status", ignorenulls=True).over(w2).alias("last_known_status"),
    )
)
