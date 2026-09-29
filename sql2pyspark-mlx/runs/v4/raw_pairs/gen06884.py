from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").asc_nulls_last())
w3 = Window.orderBy("balance", "id")

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2024-04-16").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.substring("account_type", 1, 3).alias("account_type_prefix"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

result = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("balance_cume_dist", F.cume_dist().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)
