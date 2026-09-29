from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")

ranked = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select("id", "customer_id", "account_type", "balance", "opened_date", "rn")
    .orderBy(F.col("rn").desc())
)
