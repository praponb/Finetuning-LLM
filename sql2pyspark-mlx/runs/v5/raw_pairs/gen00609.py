from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id", "status").orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())
w4 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc_nulls_first(), "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("discount_cume_dist").desc_nulls_last(), "id")
w6 = Window.partitionBy("customer_id").orderBy("discount_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("dedup_rank", "id")
w9 = Window.partitionBy("customer_id").orderBy(F.col("first_known_discount").desc_nulls_first())
w_running = Window.orderBy(F.col("customer_id_max_discount_times_next_discount").desc(), "id")
w_recent = Window.orderBy("first_known_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("first_known_discount", "id")

scored = (
    df_orders
    .filter(F.col("status") != "SHIPPED")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        F.corr("shipping_fee", "discount").over(w_part).alias("corr_shipping_fee_discount"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_6"),
        F.cume_dist().over(w3).alias("discount_cume_dist"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "discount", "discount_cume_dist")
)

ranked_orders = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked_orders
    .select(
        "*",
        F.first("discount", ignorenulls=True).over(w6).alias("first_known_discount"),
        F.last("discount", ignorenulls=True).over(w7).alias("last_known_discount"),
        F.max("discount").over(w_part).alias("customer_id_max_discount"),
        F.lead("discount", 1).over(w8).alias("next_discount"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank_2", F.rank().over(w9))
    .filter(F.col("dedup_rank_2") == 1)
    .select(
        "id",
        "discount_cume_dist",
        "first_known_discount",
        "customer_id_max_discount",
        "next_discount",
    )
)

filtered = (
    deduped
    .filter(F.col("next_discount") < 0.21)
    .withColumn("customer_id_max_discount_times_next_discount", (F.col("customer_id_max_discount") * F.col("next_discount")))
)

enriched = (
    filtered
    .select(
        "id",
        "discount_cume_dist",
        "customer_id_max_discount",
        "next_discount",
        "customer_id_max_discount_times_next_discount",
        F.ntile(4).over(w_running).alias("customer_id_max_discount_times_next_discount_bucket"),
        F.sum("customer_id_max_discount").over(w_recent).alias("cumulative_customer_id_max_discount"),
        F.lead("customer_id_max_discount_times_next_discount", 2).over(w_grp).alias("next_customer_id_max_discount_times_next_discount"),
    )
)

result = (
    enriched
    .select(
        "id",
        "discount_cume_dist",
        "customer_id_max_discount",
        "customer_id_max_discount_times_next_discount",
        "customer_id_max_discount_times_next_discount_bucket",
        "next_customer_id_max_discount_times_next_discount",
    )
)
