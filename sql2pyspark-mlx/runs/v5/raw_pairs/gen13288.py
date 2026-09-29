from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

enriched = (
    lines
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.signup_date"), F.col("b.email"), F.col("b.segment"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isin(["Bangkok", "London"]))
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.value",
        "e.signup_date",
        "e.email",
        "e.segment",
        F.col("p.name"),
        F.col("p.age"),
        F.col("p.signup_date").alias("customer_signup_date"),
    )
)

enriched_carts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.created_at",
        "s.name",
        "s.age",
        F.col("b.city"),
        F.col("b.age").alias("customer_age"),
        F.col("b.segment").alias("customer_segment"),
    )
)

valid = (
    enriched_carts
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & ~F.col("city").isin(["Bangkok", "Chiang Mai", "London"]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "name",
        "age",
        "city",
        "customer_age",
        "customer_segment",
        F.round("age", 2).alias("age_rounded"),
    )
)

scored = (
    valid
    .select(
        "customer_id",
        "city",
        "customer_segment",
        "age_rounded",
        F.min("customer_age").over(w).alias("city_min_customer_age"),
        F.max("age_rounded").over(w2).alias("age_rounded_max_15m"),
    )
)

result = (
    scored
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["segment", "customer_city"], ["segment"], []],
        F.col("dim.segment").alias("segment"),
        F.col("dim.city").alias("customer_city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.age_rounded")).alias("total_age_rounded"),
        F.max(F.col("e.age_rounded_max_15m")).alias("max_age_rounded_max_15m"),
    )
)
