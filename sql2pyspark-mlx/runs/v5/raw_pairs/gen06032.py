from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("seq_no").desc(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

deduped = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id")
)

with_dims = (
    deduped
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.amount").between(510, 1330))
    .select("a.id", "a.order_id", F.col("p.shipping_fee"), F.col("p.status"))
)

scored = (
    with_dims
    .select(
        "id",
        "order_id",
        "status",
        F.lead("shipping_fee").over(w2).alias("next_shipping_fee"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "status",
        "next_shipping_fee",
        "seq_no",
        F.when(F.col("status").rlike("^(SHIPPED|COMPLETED)$"), "CORE").when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "EXTENDED").otherwise("PRIORITY").alias("status_zone"),
        F.col("min_payment_amount"),
    )
    .orderBy("status_zone")
)
