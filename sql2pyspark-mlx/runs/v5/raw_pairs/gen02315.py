from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc_nulls_last())

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.avg("balance").over(w).alias("account_type_avg_balance"),
        F.lag("balance", 1).over(w2).alias("prev_balance"),
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status"),
        F.dense_rank().over(w4).alias("balance_dense_rank"),
    )
)
