from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("age").desc_nulls_first(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.signup_date"), F.col("d.age"), F.col("d.name"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

eligible = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") <= 320), "left_semi")
)

result = (
    eligible
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "signup_date", "age", F.col("max_account_balance"))
    .orderBy(F.col("customer_id").desc_nulls_first())
)
