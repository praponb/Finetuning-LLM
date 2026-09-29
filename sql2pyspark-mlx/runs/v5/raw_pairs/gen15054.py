from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select("id", "method", "amount", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("row_num").desc())
)
