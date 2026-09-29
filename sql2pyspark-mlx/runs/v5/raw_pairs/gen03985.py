from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last(), "id")

base = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-07-26").cast("date"))
    .select("*", F.add_months("delivered_date", 3).alias("delivered_date_plus_months"))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims = (
    deduped
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.discount") > 0.13)
    .select(
        "s.*",
        F.col("d.amount"),
        F.col("d.status").alias("order_status"),
        F.col("d.channel"),
    )
)

with_window = (
    with_dims
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.delivered_date") >= F.col("pr.starts_on")) & (F.col("src.delivered_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

by_type = (
    with_window
    .groupBy("order_status")
    .pivot("promo_code", ["VIP", "BUNDLE"])
    .agg(F.max("discount_pct").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "order_status",
        F.col("VIP_total").alias("vip_total"),
        F.col("VIP_cnt").alias("vip_cnt"),
        F.col("BUNDLE_total").alias("bundle_total"),
        F.col("BUNDLE_cnt").alias("bundle_cnt"),
    )
)

result = (
    by_type
    .select(
        "order_status",
        "bundle_total",
        "bundle_cnt",
        F.when(F.col("order_status").rlike("^(COMPLETED|SHIPPED)$"), "PRIORITY").otherwise("CORE").alias("order_status_zone"),
    )
    .orderBy(F.col("order_status").desc_nulls_last(), F.col("bundle_cnt").desc())
)
