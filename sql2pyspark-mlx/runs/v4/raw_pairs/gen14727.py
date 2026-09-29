from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel", "customer_id").orderBy(F.col("shipping_fee").asc_nulls_first())
w2 = Window.partitionBy("channel", "customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "channel")
w_grp = Window.partitionBy("channel")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-07-10"))) & F.col("channel").isNotNull() & (F.col("order_date") > "2024-09-09"))
    .select(
        "*",
        F.date_format("order_date", "yyyy-MM-dd").alias("order_date_label"),
        F.round((F.col("amount") * 100), 2).alias("amount_adjusted"),
    )
)

enriched = (
    valid
    .withColumn("shipping_fee_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("channel_max_amount_adjusted", F.max("amount_adjusted").over(w_grp))
    .withColumn("channel_count_shipping_fee", F.count("shipping_fee").over(w_grp))
)

valid_orders = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("last_known_amount") >= 1680, "High").when(F.col("last_known_amount") >= 350, "Medium").otherwise("Low").alias("last_known_amount_band"),
    )
)

result = (
    valid_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "order_date_label",
        "shipping_fee_cume_dist",
        F.when(F.col("status").rlike("^(RETURNED|CANCELLED)$"), "EXTENDED").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "GROUP_A").otherwise("CORE").alias("status_zone"),
    )
    .orderBy("id")
)
