from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "account_type", "status")
)

result = (
    ranked
    .select("id", "customer_id", "account_type")
    .orderBy(F.col("customer_id").desc())
)
