from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last(), "id")

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select("id", "account_type", "opened_date")
    .orderBy(F.col("id").asc_nulls_first(), "account_type")
    .limit(10)
)
