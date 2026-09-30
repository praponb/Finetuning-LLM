from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("discount", "id")
w3 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc())
w_part = Window.partitionBy("product_id").orderBy("quantity")
w5 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("order_date", "id")
w7 = Window.orderBy(F.col("first_known_least_quantity_unit_price").desc(), "id")
w8 = Window.orderBy("running_max_discount", "id").rowsBetween(-3, -1)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.least("quantity", "unit_price").alias("least_quantity_unit_price"))
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.discount"), F.col("d.order_date"))
)

scored = (
    enriched
    .select(
        "id",
        "quantity",
        "discount",
        "order_date",
        F.first("least_quantity_unit_price", ignorenulls=True).over(w).alias("first_known_least_quantity_unit_price"),
        F.row_number().over(w_running).alias("event_seq"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
        F.rank().over(w_part).alias("quantity_rank"),
    )
)

enriched_order_items = (
    scored
    .filter(F.col("discount").between(0.12, 0.28))
    .select(
        "id",
        "discount",
        "first_known_least_quantity_unit_price",
        "quantity_cume_dist",
        F.avg("discount").over(w5).alias("running_avg_discount"),
        F.count("discount").over(w5).alias("running_count_discount"),
        F.max("discount").over(w5).alias("running_max_discount"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

enriched_order_items_2 = (
    enriched_order_items
    .select(
        "id",
        "discount",
        "running_count_discount",
        "running_max_discount",
        "event_seq_2",
        F.row_number().over(w7).alias("seq_no"),
        F.avg("event_seq_2").over(w8).alias("event_seq_2_avg_3_prior"),
    )
)

base_order_items = (
    enriched_order_items_2
    .filter(F.col("seq_no").isNotNull())
    .withColumn("seq_no_plus_discount", (F.col("seq_no") + F.col("discount")))
)

valid = (
    base_order_items
    .filter(F.col("event_seq_2").between(3, 6))
    .select(
        "id",
        "discount",
        "running_max_discount",
        "event_seq_2",
        "seq_no",
        "event_seq_2_avg_3_prior",
        F.col("event_seq_2").cast("double").alias("event_seq_2_str"),
        F.round("seq_no", 2).alias("seq_no_rounded"),
    )
)

result = (
    valid
    .select(
        "id",
        "discount",
        "running_max_discount",
        "event_seq_2",
        "event_seq_2_avg_3_prior",
        "event_seq_2_str",
    )
    .orderBy(F.col("event_seq_2_avg_3_prior").asc_nulls_last())
)
