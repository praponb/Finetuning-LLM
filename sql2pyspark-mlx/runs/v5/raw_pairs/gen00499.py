from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status")

with_last = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        F.col("c.paid_at"),
    )
)

kept_keys = (
    with_last
    .select("id", "status")
    .intersectAll(
        df_orders
        .filter(F.col("status") != "CANCELLED")
        .select("id", "status")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "store_id",
        "order_date",
        "status",
        "discount",
        "paid_at",
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.corr("discount", "shipping_fee").over(w2).alias("corr_discount_shipping_fee"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.store_name"), F.col("p2.region"))
)

result = (
    joined
    .select(
        "region",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount_share").alias("AMOUNT_SHARE_VAL"),
    )
    .unpivot(
        ids=["region"],
        values=["DISCOUNT_VAL", "AMOUNT_SHARE_VAL"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
