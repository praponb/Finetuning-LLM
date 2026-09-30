from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "*",
        F.stddev_samp("balance").over(w).alias("customer_id_stddev_samp_balance"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
