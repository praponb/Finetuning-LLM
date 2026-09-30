from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w_recent = Window.partitionBy("status").orderBy("order_date", "id")

deduped = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("row_num_2", F.row_number().over(w2))
    .filter(F.col("row_num_2") == 1)
)

valid = (
    latest
    .filter((~F.col("method").eqNullSafe(F.lit("promptpay"))) & (F.col("paid_at") > F.lit("2023-08-14").cast("date")))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "row_num",
        "row_num_2",
        F.when(F.col("method") == "card", 1).otherwise(0).alias("is_card"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select("e.*", F.col("o.order_date"))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "is_card",
        F.lag("amount").over(w_recent).alias("prev_amount"),
        F.lead("amount").over(w_recent).alias("next_amount"),
        F.lag("is_card").over(w_recent).alias("prev_is_card"),
        F.lead("is_card").over(w_recent).alias("next_is_card"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)
