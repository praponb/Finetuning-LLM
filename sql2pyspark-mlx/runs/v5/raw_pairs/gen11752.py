from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id", "status").orderBy("paid_at", "id")

base = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-07-25 15:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.trim("method").alias("clean_method"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

result = (
    base
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "paid_at",
        "status",
        "resolved_method",
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)
