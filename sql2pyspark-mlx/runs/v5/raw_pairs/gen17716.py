from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("prev_shipping_fee").desc_nulls_first())
w3 = Window.partitionBy("status", "customer_id").orderBy("shipping_fee", "id")
w4 = Window.partitionBy("channel").orderBy("discount", "id")

metrics = (
    df_orders
    .filter(F.col("customer_id") == 18)
    .withColumn("next_shipping_fee", F.lead("shipping_fee").over(w))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w))
    .withColumn("next_amount", F.lead("amount").over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.shipped_date") >= F.col("s.order_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.cost") < 45), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "next_amount",
        "prev_amount",
        F.dense_rank().over(w2).alias("prev_shipping_fee_dense_rank"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "prev_amount",
        "prev_shipping_fee_dense_rank",
        "event_seq",
        "row_seq",
        F.when(F.col("status").rlike("^(SHIPPED|PENDING)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("event_seq").desc(), "order_date")
)
