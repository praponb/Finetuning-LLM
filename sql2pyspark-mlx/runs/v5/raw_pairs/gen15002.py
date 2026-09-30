from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").asc_nulls_first())

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-03-23")))
    .select(
        "*",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "account_type", "balance", "opened_date_label", "resolved_status")
)
