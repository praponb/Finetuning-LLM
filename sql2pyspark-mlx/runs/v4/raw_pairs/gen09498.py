from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
