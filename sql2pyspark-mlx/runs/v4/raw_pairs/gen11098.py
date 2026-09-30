from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_first(), "id")

deduped = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = deduped.select("id", "customer_id", "balance", "opened_date", "rn")
