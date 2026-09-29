from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("method") == "paypal")
    .select("id", "amount", F.percent_rank().over(w).alias("amount_pct_rank"))
    .orderBy(F.col("amount_pct_rank").desc())
)
