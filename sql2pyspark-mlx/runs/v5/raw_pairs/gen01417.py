from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select("id", "txn_type", "txn_time", F.ntile(4).over(w).alias("txn_time_ntile"))
    .orderBy(F.col("id").desc())
)
