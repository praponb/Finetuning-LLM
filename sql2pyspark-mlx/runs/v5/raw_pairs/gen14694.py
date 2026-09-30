from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("tag", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("tag", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

filtered = (
    exploded
    .filter((F.col("tag") == "BUNDLE") & F.col("tag").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.coalesce("tag", F.lit("NONE")).alias("tag_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("days_since_created_at").over(w).alias("days_since_created_at_avg_1h"),
        F.count("days_since_created_at").over(w).alias("days_since_created_at_count_1h"),
        F.max("days_since_created_at").over(w2).alias("days_since_created_at_max_2h"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.signup_date"))
)

enriched = (
    joined
    .select(
        "customer_id",
        "created_at",
        "days_since_created_at_avg_1h",
        "days_since_created_at_count_1h",
        F.sum("days_since_created_at_max_2h").over(w3).alias("days_since_created_at_max_2h_sum_30m"),
    )
)

joined_carts = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.credit_limit").between(9400, 15400))
    .select("a.*", F.col("p.name"))
)

result = (
    joined_carts
    .select(
        "created_at",
        "days_since_created_at_avg_1h",
        "days_since_created_at_count_1h",
        "days_since_created_at_max_2h_sum_30m",
        "name",
    )
    .orderBy(F.col("days_since_created_at_max_2h_sum_30m").asc_nulls_last())
)
