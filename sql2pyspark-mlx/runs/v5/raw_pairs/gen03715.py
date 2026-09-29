from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .select("id", "merchant", "txn_time", F.ntile(10).over(w).alias("quartile"))
    .orderBy(F.col("id").desc())
)
