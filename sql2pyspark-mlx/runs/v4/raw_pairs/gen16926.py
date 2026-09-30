from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("method")

result = (
    df_payments
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.count("amount").over(w2).alias("status_count_amount"),
        F.stddev_samp("amount").over(w3).alias("method_stddev_samp_amount"),
        F.sum("amount").over(w3).alias("method_sum_amount"),
    )
)
