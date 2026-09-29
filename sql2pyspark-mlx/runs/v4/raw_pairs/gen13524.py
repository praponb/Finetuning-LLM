from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2024-06-01")
    .select("id", "status", "amount", F.rank().over(w).alias("rnk"))
)
