from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .withColumn("next_balance", F.lead("balance", 1).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)
