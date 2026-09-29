from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "merchant", "amount")
    .orderBy(F.col("amount").desc())
)
