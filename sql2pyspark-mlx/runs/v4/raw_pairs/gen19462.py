from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_accounts
    .select(
        "*",
        F.ntile(10).over(w).alias("balance_bucket"),
        F.sum("balance").over(w2).alias("running_balance"),
    )
)

result = (
    metrics
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_bucket",
        "running_balance",
    )
    .orderBy(F.col("opened_date").desc())
)
