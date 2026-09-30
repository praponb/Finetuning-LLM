from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("created_at", "id", "key")
w2 = Window.partitionBy("key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

with_dims = (
    lines
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("a.*", F.col("b.credit_limit"))
)

gaps = (
    with_dims
    .withColumn("is_new_session", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

joined = (
    sessionized
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.key",
        "a.credit_limit",
        "a.session_id",
        F.col("d.city"),
        F.col("d.age"),
        F.col("d.credit_limit").alias("customer_credit_limit"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.key",
        "e.credit_limit",
        "e.session_id",
        F.col("b.city").alias("customer_city"),
    )
)

matched_keys = (
    enriched
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.col("created_at") >= "2025-11-01")
        .select("customer_id", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims_carts = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.key",
        "e.credit_limit",
        F.col("p.city"),
        F.col("p.email"),
        F.col("p.age"),
    )
)

result = (
    with_dims_carts
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "key",
        "credit_limit",
        "age",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("credit_limit").asc_nulls_first(), "customer_id")
)
