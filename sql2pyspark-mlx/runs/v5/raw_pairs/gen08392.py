from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("store_id", "channel")
w3 = Window.partitionBy("customer_id", "channel").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("order_date", "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-05-21").cast("date")) & (F.col("amount") > 870))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        F.length("status").alias("status_length"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "discount",
        "status_length",
        F.max("amount").over(w).alias("customer_id_max_amount"),
        F.corr("amount", "discount").over(w2).alias("corr_amount_discount"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.count("discount").over(w3).alias("running_count_discount"),
        F.avg("discount").over(w3).alias("running_avg_discount"),
        F.min("discount").over(w3).alias("running_min_discount"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

attributed = (
    enriched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "promo_id",
        "promo_code",
        "discount_pct",
        "running_avg_discount",
        "discount",
    )
    .unpivot(
        ids=["id", "promo_id", "promo_code"],
        values=["discount_pct", "running_avg_discount", "discount"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
