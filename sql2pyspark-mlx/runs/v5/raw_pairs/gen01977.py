from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

exploded = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

metrics = (
    exploded
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .withColumn("product_id_max_2h", F.max("product_id").over(w))
)

enriched = (
    metrics
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.product_id",
        "e.price",
        "e.product_id_max_2h",
        F.col("p.country"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.customer_id",
        "s.product_id_max_2h",
        "s.country",
        F.col("p.signup_date"),
        F.col("p.city"),
        F.col("p.email"),
    )
)

result = (
    with_dims
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "country",
        "signup_date",
        "city",
        "email",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
