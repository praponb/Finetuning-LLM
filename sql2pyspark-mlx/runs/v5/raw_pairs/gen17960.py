from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.orderBy("opened_date", "id")

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select("*", F.col("balance").cast("int").alias("balance_int"))
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "balance_int",
        F.lag("balance", 1).over(w).alias("prev_balance"),
        F.row_number().over(w2).alias("seq_no"),
        F.lag("balance_int", 1, 0).over(w3).alias("prev_balance_int"),
    )
)
