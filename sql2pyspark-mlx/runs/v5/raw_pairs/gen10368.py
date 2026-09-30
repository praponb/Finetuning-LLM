from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select("id", "status", "balance", F.ntile(2).over(w).alias("balance_ntile"))
)
