from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-03-28")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
)
