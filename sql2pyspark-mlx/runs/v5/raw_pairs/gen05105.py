from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").asc_nulls_last())
w3 = Window.partitionBy("customer_id")

scored = (
    df_accounts
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w))
    .withColumn("balance_percent_rank", F.percent_rank().over(w2))
    .withColumn("customer_id_count_balance", F.count("balance").over(w3))
)

result = (
    scored
    .select(
        "id",
        "account_type",
        "balance",
        "first_known_status",
        "balance_percent_rank",
        "customer_id_count_balance",
    )
    .orderBy("account_type")
)
