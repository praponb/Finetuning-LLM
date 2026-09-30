from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "element")
w2 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc())
w3 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit_whole").asc_nulls_last())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

with_dims = (
    flattened
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.credit_limit"))
)

with_windows = (
    with_dims
    .select(
        "customer_id",
        "created_at",
        "credit_limit",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
    )
)

base = (
    with_windows
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 17:00:00"))) & F.col("customer_id").isNotNull())
    .withColumn("credit_limit_whole", F.ceil("credit_limit"))
)

enriched = (
    base
    .select(
        "credit_limit_share",
        "credit_limit_whole",
        F.rank().over(w2).alias("credit_limit_rank"),
        F.rank().over(w3).alias("credit_limit_whole_rank"),
    )
)

result = (
    enriched
    .select("credit_limit_whole", "credit_limit_rank", "credit_limit_whole_rank")
)
