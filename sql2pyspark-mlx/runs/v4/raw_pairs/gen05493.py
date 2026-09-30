from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("qty")
w3 = Window.orderBy("price")
w4 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_max_price_share").desc_nulls_last())
w5 = Window.orderBy(F.col("qty_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("customer_id_sum_qty").desc_nulls_first())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select("id", "customer_id", "coupon_codes", F.col("item.qty"), F.col("item.price"))
)

scored = (
    flattened
    .select(
        "*",
        F.sum("qty").over(w).alias("customer_id_sum_qty"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.rank().over(w2).alias("qty_rank"),
    )
)

remaining_keys = (
    scored
    .select("customer_id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("customer_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored_carts = (
    reconciled
    .withColumn("price_percent_rank", F.percent_rank().over(w3))
    .withColumn("customer_id_max_price_share", F.max("price_share").over(w))
)

filtered = (
    scored_carts
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "customer_id",
        "qty",
        "customer_id_sum_qty",
        "price_share",
        "qty_share",
        "qty_rank",
        "customer_id_max_price_share",
        F.round("qty", 2).alias("qty_rounded"),
    )
)

scored_carts_2 = (
    filtered
    .withColumn("customer_id_max_price_share_rank", F.rank().over(w4))
    .withColumn("qty_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("customer_id_sum_qty_cume_dist", F.cume_dist().over(w6))
)

result = (
    scored_carts_2
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "customer_id_sum_qty",
        "price_share",
        "qty_rank",
        "customer_id_max_price_share",
        "qty_rounded",
        "qty_rank_percent_rank",
        "customer_id_sum_qty_cume_dist",
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("customer_id_sum_qty").desc_nulls_first(), "qty_rounded")
)
