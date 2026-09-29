from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("amount").between(840, 1160))
    .select("id", "status", "amount", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
)
