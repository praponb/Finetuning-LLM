from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_day").orderBy(F.col("num_cities").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

joined = (
    flattened
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("s.*", F.col("b.city"), F.col("b.country"), F.col("b.age"))
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.country").alias("customer_country"),
        F.col("b.signup_date"),
        F.col("b.credit_limit"),
    )
)

result = (
    joined_carts
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.when(F.col("age") >= 57, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.max(F.when(F.col("value") == "VIP", F.col("age"))).alias("vip_max_age"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count(F.when(F.col("value") == "SPRING10", 1)).alias("spring10_rows"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .withColumn("num_cities_percent_rank", F.percent_rank().over(w))
)
