from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

joined = (
    flattened
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.email"), F.col("p.name"), F.col("p.segment"))
)

cleaned = (
    joined
    .filter((F.col("element").eqNullSafe(F.lit("BUNDLE"))) & F.col("customer_id").isNotNull())
    .select("*", F.initcap(F.trim("name")).alias("name_title"))
)

joined_carts = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        F.col("p.credit_limit"),
        F.col("p.age"),
        F.col("p.city"),
    )
)

with_windows = (
    joined_carts
    .select(
        "id",
        "customer_id",
        "credit_limit",
        F.stddev_samp("credit_limit").over(w).alias("city_stddev_samp_credit_limit"),
    )
)

result = (
    with_windows
    .select("id", "credit_limit", "city_stddev_samp_credit_limit")
    .orderBy(F.col("id").desc_nulls_last())
)
