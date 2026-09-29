from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("channel")
w_grp = Window.partitionBy("store_id").orderBy(F.col("prev_amount").asc_nulls_last())
w_hist = Window.partitionBy("store_id", "customer_id").orderBy(F.col("event_seq").asc_nulls_first())
w6 = Window.orderBy(F.col("event_seq").asc_nulls_first())
w7 = Window.partitionBy("store_id", "channel")

base = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("status") == "COMPLETED"))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

metrics = (
    base
    .select(
        "*",
        F.row_number().over(w_part).alias("event_seq"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
    )
)

matched_keys = (
    metrics
    .select("status", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("status", "customer_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("corr_discount_amount", F.corr("discount", "amount").over(w3))
    .withColumn("prev_amount_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("event_seq_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w6))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w7)), 4))
)
