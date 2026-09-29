from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first(), "id")

ranked = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "amount")
)

ranked_payments = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_payments_2 = (
    ranked_payments
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked_payments_2
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.order_date"))
)

filtered = (
    enriched
    .filter(F.col("order_id").isNotNull())
    .withColumn("order_date_label", F.date_format("order_date", "MMM yyyy"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.order_date",
        "src.order_date_label",
        F.col("b.channel"),
        F.col("b.amount").alias("order_amount"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("order_amount") >= 1450, "LARGE").otherwise("SMALL").alias("order_amount_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.sum("order_amount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("order_amount_per_store"),
        F.percentile_approx("order_amount", 0.5).alias("order_amount_p50"),
    )
    .filter(F.col("order_amount_per_store") > 0)
)
