from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy(F.col("balance").desc(), "id")
w_part = Window.partitionBy("customer_id", "status").orderBy(F.col("adjusted_balance").desc(), "id")
w_grp = Window.partitionBy("status").orderBy("adjusted_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-05-18")))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.round((F.col("balance") * 1.07), 2).alias("adjusted_balance"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.last("adjusted_balance", ignorenulls=True).over(w).alias("last_known_adjusted_balance"),
        F.avg("balance").over(w2).alias("customer_id_avg_balance"),
        F.ntile(3).over(w3).alias("balance_bucket"),
        F.row_number().over(w_part).alias("event_seq"),
        F.first("status", ignorenulls=True).over(w_grp).alias("first_known_status"),
    )
)
