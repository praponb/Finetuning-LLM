from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

with_dims = (
    exploded
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.name"), F.col("d.segment"), F.col("d.signup_date"))
)

with_dims_carts = (
    with_dims
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.created_at",
        "src.segment",
        F.col("d.name").alias("customer_name"),
        F.col("d.credit_limit"),
    )
)

scored = (
    with_dims_carts
    .select(
        "id",
        "customer_id",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.avg("credit_limit").over(w).alias("segment_avg_credit_limit"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.city").isin(["Bangkok", "Berlin"]))
    .select(
        "src.id",
        "src.customer_id",
        "src.credit_limit_share",
        "src.segment_avg_credit_limit",
        F.col("c.credit_limit"),
        F.col("c.segment"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.email"), F.col("b.name"))
)

with_dims_carts_2 = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.name").alias("customer_name"))
)

result = (
    with_dims_carts_2
    .select(
        "credit_limit_share",
        "segment_avg_credit_limit",
        "credit_limit",
        "segment",
        "email",
        "name",
    )
    .orderBy(F.col("segment").asc_nulls_first())
)
