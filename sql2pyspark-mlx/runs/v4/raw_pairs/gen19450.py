from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select("id", "status", "amount", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "status", "amount")
)
