from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("next_discount").desc(), "id")
w3 = Window.partitionBy("status", "customer_id").orderBy("row_seq", "id")

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("channel") == "partner"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1340, "High").when(F.col("amount") >= 640, "Medium").otherwise("Low"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

scored = (
    cleaned
    .select(
        "id",
        "customer_id",
        "status",
        "amount",
        F.lead("discount").over(w_grp).alias("next_discount"),
        F.lag("amount").over(w_grp).alias("prev_amount"),
        F.lead("amount").over(w_grp).alias("next_amount"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.status").isin(["SUCCESS", "FAILED"])), "left_semi")
)

ranked = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .filter(F.col("dedup_rank") <= 1)
    .select(
        "id",
        "status",
        "amount",
        "next_discount",
        "prev_amount",
        "row_seq",
        F.lead("next_amount").over(w3).alias("next_next_amount"),
        F.lag("next_discount").over(w3).alias("prev_next_discount"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
