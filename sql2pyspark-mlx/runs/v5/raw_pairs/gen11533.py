from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_accounts
    .filter(F.col("balance") <= 20000)
    .withColumn("running_avg_balance", F.avg("balance").over(w_part))
    .withColumn("running_min_balance", F.min("balance").over(w_part))
    .withColumn("running_count_balance", F.count("balance").over(w_part))
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "running_min_balance",
        F.when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "PRIORITY").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "CORE").otherwise("EXTENDED").alias("account_type_zone"),
    )
    .orderBy("opened_date")
)
