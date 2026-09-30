from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("amount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-05-13 17:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.when(F.col("merchant").isin(["Starbucks", "Lazada"]), "Core").otherwise("Other").alias("merchant_group"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    valid
    .select(
        "*",
        F.last("amount_int", ignorenulls=True).over(w_running).alias("last_known_amount_int"),
        F.max("amount_int").over(w2).alias("amount_int_max_90d"),
    )
)
