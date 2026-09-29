from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-08").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        F.when(F.col("balance") >= 86000, "High").when(F.col("balance") >= 14000, "Medium").otherwise("Low").alias("balance_band"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)
