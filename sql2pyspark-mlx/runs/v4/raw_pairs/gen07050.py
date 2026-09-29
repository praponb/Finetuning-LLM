from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
)
