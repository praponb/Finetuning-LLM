from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")

result = (
    df_accounts
    .select(
        "*",
        F.first("status", ignorenulls=True).over(w).alias("first_known_status"),
        F.count("balance").over(w2).alias("customer_id_count_balance"),
    )
)
