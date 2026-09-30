from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.id", "a.customer_id", F.col("b.age"), F.col("b.city"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        F.col("p.segment"),
        F.col("p.signup_date"),
        F.col("p.name"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.credit_limit"), F.col("p.name").alias("customer_name"))
)

scored = (
    with_dims
    .select(
        "id",
        "customer_id",
        "segment",
        "credit_limit",
        F.avg("credit_limit").over(w).alias("customer_id_avg_credit_limit"),
    )
)

joined_carts = (
    scored
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("a.id", "a.customer_id", "a.segment", F.col("b.age"))
)

result = (
    joined_carts
    .groupBy("customer_id")
    .pivot("segment", ["Consumer", "SMB"])
    .agg(F.max("age").alias("value_sum"))
)
