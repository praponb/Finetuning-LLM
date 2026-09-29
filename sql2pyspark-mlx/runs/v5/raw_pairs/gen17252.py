from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct").asc_nulls_last())
w2 = Window.orderBy("amount", "id", "promo_id")
w3 = Window.partitionBy("order_status").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w4 = Window.partitionBy("status").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w_running = Window.partitionBy("order_id").orderBy("discount_pct_cume_dist", "id", "promo_id")

with_dims = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.delivered_date",
        "a.cost",
        "a.status",
        F.col("b.status").alias("order_status"),
        F.col("b.amount"),
        F.col("b.channel"),
    )
)

attributed = (
    with_dims
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.delivered_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    attributed
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w))
    .withColumn("prev_cost", F.lag("cost", 1).over(w2))
    .withColumn("discount_pct_bucket", F.ntile(3).over(w3))
)

with_windows = (
    metrics
    .select(
        "*",
        F.ntile(3).over(w4).alias("promo_id_bucket"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "order_status",
        "amount",
        "channel",
        "promo_id",
        "promo_code",
        F.when(F.col("order_status").rlike("^(SHIPPED|COMPLETED)$"), "CORE").when(F.col("order_status").rlike("^(PENDING|CANCELLED)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("order_status_zone"),
    )
    .orderBy(F.col("cost").desc())
)
