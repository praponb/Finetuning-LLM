from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("order_date", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("order_date").desc(), "id")

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "delivered_date")
)

enriched = (
    deduped
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.id", "e.order_id", F.col("d.order_date"))
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.discount") <= 0.22)
    .select(
        "e.id",
        "e.order_id",
        "e.row_seq_2",
        "e.seq_no",
        F.col("b.status"),
        F.col("b.amount"),
    )
)

result = (
    joined
    .select(
        "id",
        "row_seq_2",
        "seq_no",
        "status",
        "amount",
        F.when(F.col("status").rlike("^(COMPLETED|CANCELLED)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
