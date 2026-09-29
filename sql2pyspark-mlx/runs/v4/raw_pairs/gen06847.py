from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

ranked = (
    df_accounts
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .select("id", "account_type", "opened_date", "status")
    .orderBy(F.col("opened_date").asc_nulls_first(), "status")
)
