from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

result = (
    latest_match
    .groupBy("account_type")
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_rows"),
        F.count("*").alias("row_count"),
    )
    .orderBy("checking_rows")
)
