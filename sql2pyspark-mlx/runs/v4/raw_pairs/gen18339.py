from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select("id", "amount", F.percent_rank().over(w).alias("amount_pct_rank"))
)
