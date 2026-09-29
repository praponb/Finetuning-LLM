from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2024-02-02").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.coalesce("status", F.lit("Unknown")).alias("status_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)
