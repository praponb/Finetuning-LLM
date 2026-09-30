from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("balance_avg_3", F.avg("balance").over(w2))
    .withColumn("balance_avg_2", F.avg("balance").over(w3))
    .withColumn("cumulative_balance", F.sum("balance").over(w4))
)

result = (
    enriched
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "seq_no",
        "balance_avg_3",
        "cumulative_balance",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "GROUP_B").when(F.col("status").rlike("^(CLOSED)$"), "GROUP_A").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("seq_no").desc(), "status")
)
