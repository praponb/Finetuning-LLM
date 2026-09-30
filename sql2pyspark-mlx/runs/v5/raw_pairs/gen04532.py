from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc_nulls_first())

exploded = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("attr_key", "v"))
)

joined = (
    exploded
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.name").isNotNull())
    .select("s.*", F.col("b.credit_limit"), F.col("b.name"))
)

with_windows = (
    joined
    .select(
        "attr_key",
        "v",
        "name",
        F.percent_rank().over(w).alias("credit_limit_percent_rank"),
        F.percent_rank().over(w2).alias("credit_limit_percent_rank_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("attr_key").isNotNull())
    .select(
        "attr_key",
        "v",
        F.greatest("credit_limit_percent_rank_2", "credit_limit_percent_rank").alias("greatest_credit_limit_percent_rank_2_credit_limit_percent_rank"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_clean"),
    )
)

stats = (
    valid
    .groupBy("attr_key")
    .agg(
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.count_if(F.col("attr_key") == "gift").alias("gift_events"),
        F.percentile_approx("greatest_credit_limit_percent_rank_2_credit_limit_percent_rank", [0.25, 0.5, 0.75]).alias("greatest_credit_limit_percent_rank_2_credit_limit_percent_rank_quartiles"),
        F.count_if(F.col("attr_key") == "gift").alias("gift_events_2"),
    )
)

result = (
    stats
    .select(
        "attr_key",
        "num_attr_keys",
        "gift_rows",
        "gift_events",
        "greatest_credit_limit_percent_rank_2_credit_limit_percent_rank_quartiles",
    )
)
