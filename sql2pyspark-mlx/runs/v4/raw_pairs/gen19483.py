from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id", "order_id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked_order_items = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    ranked_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([15, 24, 25]))
        .select("id", "order_id")
    )
)

reconciled = (
    ranked_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.max("unit_price").over(w3).alias("order_id_max_unit_price"),
        F.percent_rank().over(w4).alias("quantity_percent_rank"),
        F.percent_rank().over(w5).alias("quantity_percent_rank_2"),
        F.corr("unit_price", "quantity").over(w6).alias("corr_unit_price_quantity"),
        F.min("unit_price").over(w7).alias("product_id_min_unit_price"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select("e.*", F.col("o.channel"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("unit_price_int") >= 70, "PREMIUM").otherwise("STANDARD").alias("unit_price_int_tier"),
        F.when(F.col("product_id_min_unit_price") >= 428, "HIGH").otherwise("LOW").alias("product_id_min_unit_price_tier"),
        F.when(F.col("quantity") >= 8, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("unit_price_int_tier").asc_nulls_last())
)
