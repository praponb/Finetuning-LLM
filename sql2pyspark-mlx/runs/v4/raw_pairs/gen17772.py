from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_last(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-06-24 22:00:00").cast("timestamp")) & F.col("order_id").isin([1, 7, 8, 34]))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.trim("method").alias("clean_method"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
