from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

enriched = (
    df_accounts
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w))
    .withColumn("balance_bucket", F.ntile(4).over(w2))
    .withColumn("balance_percent_rank", F.percent_rank().over(w3))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "first_known_balance",
    )
)
