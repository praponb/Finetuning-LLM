from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status")

valid = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.lower("status").alias("status_lower"),
        F.round("balance", 1).alias("balance_rounded"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

result = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("balance_rank"),
        F.avg("balance_rounded").over(w2).alias("balance_rounded_avg_3"),
        F.max("balance_rounded").over(w3).alias("customer_id_max_balance_rounded"),
    )
)
