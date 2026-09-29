from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy("days_since_order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("days_since_order_date").asc_nulls_last())

filtered = (
    df_orders
    .filter(F.col("channel").eqNullSafe(F.lit("mobile")))
    .select(
        "*",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.when(F.col("shipping_fee") >= 44, "High").when(F.col("shipping_fee") >= 11, "Medium").otherwise("Low").alias("shipping_fee_band"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.lag("days_since_order_date").over(w).alias("prev_days_since_order_date"),
        F.lag("discount").over(w).alias("prev_discount"),
        F.lead("discount").over(w).alias("next_discount"),
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("days_since_order_date_dense_rank"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id").isin([28, 35, 39]))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched = (
    verified
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select("customer_id", "next_discount", "amount", "discount_pct", "prev_discount")
    .unpivot(
        ids=["customer_id"],
        values=["next_discount", "amount", "discount_pct", "prev_discount"],
        variableColumnName="measure",
        valueColumnName="val",
    )
)
