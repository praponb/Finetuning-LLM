from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("balance").asc_nulls_first())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("balance").desc(), "id")
w_grp_2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .filter(F.col("balance").between(75000, 76000))
    .select(
        "id",
        "account_type",
        "status",
        F.rank().over(w_part).alias("balance_rank"),
        F.sum("balance").over(w2).alias("status_sum_balance"),
        F.sum("balance").over(w3).alias("cumulative_balance"),
        F.ntile(3).over(w_grp).alias("balance_bucket"),
        F.first("status", ignorenulls=True).over(w_grp_2).alias("first_known_status"),
    )
)
