from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id")
w_ordered_2 = Window.partitionBy("channel")

base = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-09-17")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        F.add_months("order_date", 6).alias("order_date_plus_months"),
    )
)

kept_keys = (
    base
    .select("store_id", "customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("amount") < 1220)
        .select("store_id", "customer_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.corr("discount", "amount").over(w_ordered).alias("corr_discount_amount"),
        F.count("discount").over(w_ordered_2).alias("channel_count_discount"),
        F.count("amount").over(w_ordered).alias("customer_id_count_amount"),
    )
)

result = (
    scored
    .groupBy("store_id")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.sum("corr_discount_amount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("corr_discount_amount_per_mobile"),
        F.avg("customer_id_count_amount").alias("avg_customer_id_count_amount"),
    )
    .filter(F.col("num_channels") > 1)
    .orderBy(F.col("num_channels").desc_nulls_last())
)
