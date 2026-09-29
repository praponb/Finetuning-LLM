from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last(), "id")

filtered = (
    df_accounts
    .filter(F.col("status") != "CLOSED")
    .withColumn("opened_date_period", F.date_trunc("YEAR", "opened_date"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
