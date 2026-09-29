from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("balance_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .withColumn("balance_filled", F.coalesce("balance", F.lit(0)))
    .withColumn("status_account_type", F.concat("status", F.lit(" "), "account_type"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("N/A")))
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "balance_filled",
        "resolved_status",
        F.avg("balance_filled").over(w).alias("running_avg_balance_filled"),
        F.min("balance_filled").over(w).alias("running_min_balance_filled"),
        F.max("balance_filled").over(w).alias("running_max_balance_filled"),
    )
)
