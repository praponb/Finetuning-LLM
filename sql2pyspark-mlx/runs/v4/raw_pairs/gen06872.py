from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")

result = (
    df_transactions
    .select("id", "merchant", F.count("*").over(w).alias("merchant_count"))
)
