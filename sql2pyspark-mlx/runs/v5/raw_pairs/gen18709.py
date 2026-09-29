from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("shipping_fee", "id", "promo_id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id", "promo_id")
w3 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id"), F.col("s.promo_id_2")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

matched = (
    df_orders
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    matched
    .withColumn("last3_min_promo_id", F.min("promo_id").over(w_hist))
    .withColumn("last3_max_promo_id", F.max("promo_id").over(w_hist))
    .withColumn("last3_avg_promo_id", F.avg("promo_id").over(w_hist))
    .withColumn("last3_count_promo_id", F.count("promo_id").over(w_hist))
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_window = (
    ranked
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest_match = (
    with_window
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.status",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.promo_id",
        "s.discount_pct",
        "s.last3_min_promo_id",
        "s.last3_max_promo_id",
        "s.last3_avg_promo_id",
        "s.rank_in_group",
        "s.promo_id_2",
        "s.promotion_promo_code",
        F.col("c.quantity"),
    )
)

result = (
    latest_match
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "shipping_fee",
        "promo_id",
        "discount_pct",
        "last3_max_promo_id",
        F.when(F.col("status").rlike("^(PENDING|COMPLETED)$"), "PRIORITY").otherwise("GROUP_A").alias("status_zone"),
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("amount").desc())
)
