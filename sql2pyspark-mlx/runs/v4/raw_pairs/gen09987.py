from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

valid = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.abs("balance").alias("abs_balance"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
