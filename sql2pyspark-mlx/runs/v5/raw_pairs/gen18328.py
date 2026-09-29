from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("key")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("age").desc())
w4 = Window.partitionBy("segment").orderBy(F.col("key_min_days_since_created_at").desc_nulls_first())
w_hist = Window.partitionBy("customer_id", "city")
w_ordered = Window.orderBy(F.col("key_min_days_since_created_at").desc())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

cleaned = (
    flattened
    .filter((F.col("key").eqNullSafe(F.lit("gift"))) & F.col("customer_id").isNotNull())
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
)

metrics = (
    cleaned
    .filter(F.col("days_since_created_at").between(27, 38))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "days_since_created_at",
        F.stddev_samp("days_since_created_at").over(w_running).alias("key_stddev_samp_days_since_created_at"),
        F.min("days_since_created_at").over(w_running).alias("key_min_days_since_created_at"),
        F.max("days_since_created_at").over(w2).alias("days_since_created_at_max_30m"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.name"), F.col("c.city"), F.col("c.country"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.age"), F.col("b.segment"), F.col("b.credit_limit"))
)

scored = (
    joined
    .select(
        "id",
        "name",
        "country",
        F.cume_dist().over(w3).alias("age_cume_dist"),
        F.dense_rank().over(w4).alias("key_min_days_since_created_at_dense_rank"),
        F.min("days_since_created_at_max_30m").over(w_hist).alias("customer_id_min_days_since_created_at_max_30m"),
        F.dense_rank().over(w_ordered).alias("key_min_days_since_created_at_dense_rank_2"),
    )
)

daily = (
    scored
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("age_cume_dist")).otherwise(0)).alias("uk_age_cume_dist"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("uk_age_cume_dist") > 0)
)

result = (
    daily
    .select(
        "country",
        "uk_age_cume_dist",
        "num_rows",
        F.when(F.col("country").rlike("^(US|JP)$"), "CORE").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("uk_age_cume_dist").desc_nulls_first())
)
