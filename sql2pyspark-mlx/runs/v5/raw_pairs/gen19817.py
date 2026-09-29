from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "amount", F.rank().over(w).alias("amount_rank"))
    .orderBy(F.col("amount_rank").desc(), "id")
)
