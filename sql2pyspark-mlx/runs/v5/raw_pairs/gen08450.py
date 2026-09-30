from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer"]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
