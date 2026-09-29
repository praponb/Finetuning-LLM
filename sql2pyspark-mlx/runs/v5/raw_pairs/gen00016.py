from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CREDIT"]) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.upper("status").alias("status_upper"),
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"))
)
