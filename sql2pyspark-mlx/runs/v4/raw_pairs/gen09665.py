from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, -1)
w3 = Window.orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("store_id", "status").orderBy(F.col("amount").desc_nulls_last())
w5 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-07-08").cast("date")) & F.col("customer_id").isNotNull() & (F.col("shipping_fee") >= 20))
    .select(
        "*",
        F.round((F.col("discount") * 1.1), 2).alias("discount_scaled"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

enriched = (
    valid
    .withColumn("customer_id_max_discount", F.max("discount").over(w))
    .withColumn("discount_avg_6_prior", F.avg("discount").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("amount_cume_dist", F.cume_dist().over(w4))
    .withColumn("shipping_fee_bucket", F.ntile(10).over(w5))
)

valid_orders = (
    enriched
    .filter(F.col("discount").between(0.15, 0.39))
    .select(
        "*",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean_2"),
    )
)

result = (
    valid_orders
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "discount_scaled",
        "customer_id_max_discount",
        F.when(F.col("channel").rlike("^(store|partner)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(web|mobile)$"), "PRIORITY").otherwise("GROUP_A").alias("channel_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
