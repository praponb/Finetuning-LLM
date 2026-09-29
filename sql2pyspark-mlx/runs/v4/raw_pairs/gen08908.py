from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Shell", "Starbucks", "7-Eleven"]))
    .select("id", "txn_type", "txn_time", F.ntile(4).over(w).alias("bucket"))
)
