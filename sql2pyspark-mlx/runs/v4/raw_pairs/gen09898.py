from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "paypal") & ((F.col("amount") <= 1480) | (F.to_date("paid_at") >= "2023-11-24")))
    .select("status", F.initcap(F.trim("status")).alias("status_title"))
)
