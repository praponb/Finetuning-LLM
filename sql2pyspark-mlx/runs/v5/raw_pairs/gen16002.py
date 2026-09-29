from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "customer_id", "opened_date")
    .orderBy(F.col("opened_date").desc())
)
