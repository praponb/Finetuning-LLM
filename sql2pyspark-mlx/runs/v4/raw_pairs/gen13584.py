from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("balance", "id")
w3 = Window.orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("balance_bucket", F.ntile(3).over(w2))
    .withColumn("balance_bucket_2", F.ntile(4).over(w3))
)
