from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-03-29")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.merchant"))
)
