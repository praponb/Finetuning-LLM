from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .select("id", "status", "amount", "method", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "status", "amount", "method")
    .orderBy("id")
)
