from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo", "customer_id")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

scored = (
    lines
    .withColumn("promo_min_price", F.min("price").over(w))
    .withColumn("price_sum_30m", F.sum("price").over(w2))
)

with_dims = (
    scored
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.customer_id", "e.promo", F.col("d.signup_date"))
)

with_dims_carts = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.email"), F.col("c.name"))
)

joined = (
    with_dims_carts
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.customer_id",
        "s.signup_date",
        "s.email",
        F.col("b.country"),
        F.col("b.segment"),
    )
)

with_dims_carts_2 = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.country").alias("customer_country"), F.col("d.city"))
)

with_dims_carts_3 = (
    with_dims_carts_2
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.customer_id",
        "src.email",
        "src.country",
        "src.customer_country",
        "src.city",
        F.col("d.credit_limit"),
        F.col("d.city").alias("customer_city"),
        F.col("d.segment").alias("customer_segment"),
    )
)

result = (
    with_dims_carts_3
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "country",
        "customer_country",
        "city",
        "credit_limit",
        "customer_city",
        "customer_segment",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("credit_limit").desc())
)
