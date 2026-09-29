from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc_nulls_last(), "id")

filtered = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.trim("method").alias("clean_method"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
