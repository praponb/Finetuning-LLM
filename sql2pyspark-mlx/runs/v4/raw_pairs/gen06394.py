from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("balance", "id")

scored = (
    df_accounts
    .withColumn("cumulative_balance", F.sum("balance").over(w))
    .withColumn("next_balance", F.lead("balance", 1).over(w2))
)

result = (
    scored
    .select("id", "customer_id", "balance", "opened_date", "status", "next_balance")
)
