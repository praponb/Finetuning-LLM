from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit_whole").asc_nulls_last())
w2 = Window.orderBy(F.col("credit_limit_whole").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("credit_limit_whole").asc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

joined = (
    exploded
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.id", "s.customer_id", "s.key", F.col("p.credit_limit"), F.col("p.city"))
)

base = (
    joined
    .filter(F.col("credit_limit") <= 7300)
    .select(
        "*",
        F.floor("credit_limit").alias("credit_limit_whole"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.is_active"))
    .select(
        "e.customer_id",
        "e.key",
        "e.city",
        "e.credit_limit_whole",
        F.col("b.country"),
    )
)

cleaned = (
    enriched
    .filter(F.col("customer_id").isNotNull() & F.col("credit_limit_whole").between(59, 76))
    .withColumn("credit_limit_whole_whole", F.floor("credit_limit_whole"))
    .withColumn("country_key_key", F.concat_ws("-", "country", "key"))
)

metrics = (
    cleaned
    .select(
        "customer_id",
        "country",
        "credit_limit_whole_whole",
        F.percent_rank().over(w).alias("credit_limit_whole_percent_rank"),
        F.rank().over(w2).alias("credit_limit_whole_rank"),
        F.rank().over(w3).alias("credit_limit_whole_rank_2"),
    )
)

valid = (
    metrics
    .filter(F.col("credit_limit_whole_rank").between(4, 8))
    .withColumn("credit_limit_whole_rank_times_credit_limit_whole_percent_rank", (F.col("credit_limit_whole_rank") * F.col("credit_limit_whole_percent_rank")))
)

grouped = (
    valid
    .rollup("customer_id")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.round(F.sum("credit_limit_whole_percent_rank") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("credit_limit_whole_percent_rank_per_uk"),
        F.round(F.sum("credit_limit_whole_percent_rank") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("credit_limit_whole_percent_rank_per_uk_2"),
        F.grouping("customer_id").alias("grouping_level"),
    )
)

result = (
    grouped
    .select("sg_count", "credit_limit_whole_percent_rank_per_uk_2")
    .orderBy(F.col("credit_limit_whole_percent_rank_per_uk_2").asc_nulls_first())
)
