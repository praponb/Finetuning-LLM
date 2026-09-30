from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "promo_code")
w2 = Window.orderBy("amount", "id", "promo_id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("customer_id").orderBy("discount_pct", "id", "promo_id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("promo_id", "id", "promo_id")
w5 = Window.partitionBy("customer_id", "status").orderBy("order_date", "id", "promo_id").rowsBetween(-2, -1)
w6 = Window.partitionBy("customer_id").orderBy(F.col("discount_pct_share").asc_nulls_first())
w7 = Window.partitionBy("promo_code").orderBy(F.col("promo_id_avg_6_prior").desc_nulls_last(), "id", "promo_id", "promo_id_2")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("units").alias("max_store_sale_units"))

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-01-05").cast("date")) & F.col("customer_id").isNotNull() & (F.col("channel") != "web"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.concat_ws(",", "channel", "status").alias("channel_status_key"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

matched = (
    valid
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    matched
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.avg("promo_id").over(w2).alias("promo_id_avg_6_prior"),
        F.avg("discount_pct").over(w_grp).alias("discount_pct_avg_3"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("promo_id").over(w5).alias("promo_id_avg_2_prior"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

attributed = (
    ranked
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked_orders = (
    attributed
    .withColumn("dedup_rank_2", F.row_number().over(w7))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

base = (
    ranked_orders
    .filter(F.col("amount").isNotNull())
    .withColumn("discount_pct_avg_3_filled", F.coalesce("discount_pct_avg_3", F.lit(0)))
    .withColumn("promo_code_title", F.initcap(F.trim("promo_code")))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

result = (
    base
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel_title",
        "channel_status_key",
        "promo_id",
        "discount_pct",
        "discount_pct_share",
        F.col("max_store_sale_units"),
    )
    .orderBy(F.col("discount_pct").desc())
)
