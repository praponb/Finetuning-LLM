from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element")
w2 = Window.orderBy("credit_limit")
w3 = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("element", "customer_id")
w6 = Window.orderBy(F.col("element_min_credit_limit").asc_nulls_last())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.customer_id", "a.created_at", "a.element", F.col("d.credit_limit"))
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").like("%Chai%"))
    .select("a.customer_id", "a.element", "a.credit_limit", F.col("c.name"))
)

enriched_carts = (
    joined
    .withColumn("element_min_credit_limit", F.min("credit_limit").over(w))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w2))
    .withColumn("credit_limit_rank", F.rank().over(w3))
    .withColumn("customer_id_avg_credit_limit", F.avg("credit_limit").over(w4))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w5)), 4))
)

with_dims = (
    enriched_carts
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.customer_id",
        "s.credit_limit",
        "s.element_min_credit_limit",
        F.col("b.segment"),
    )
)

result = (
    with_dims
    .select(
        "customer_id",
        "credit_limit",
        "segment",
        F.rank().over(w6).alias("element_min_credit_limit_rank"),
    )
)
