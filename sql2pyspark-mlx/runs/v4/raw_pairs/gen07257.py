from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-06-18").cast("date")) & F.col("status").isNotNull())
    .select(
        "*",
        F.round((F.col("balance") * 0.85), 2).alias("balance_scaled"),
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)
