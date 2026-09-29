from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_active_flag").asc_nulls_first())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("segment", "country").orderBy(F.col("is_active_flag_percent_rank").desc())
w4 = Window.partitionBy("country", "city").orderBy(F.col("is_active_flag").asc_nulls_last())

matched_keys = (
    df_customers
    .select("city")
    .subtract(
        df_customers
        .filter(F.length("name") > 7)
        .select("city")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("segment") != "Consumer")
    .withColumn("country_city", F.concat("country", F.lit("/"), "city"))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

eligible = (
    base
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left_anti")
)

enriched = (
    eligible
    .filter(F.col("name").isNotNull())
    .select(
        "*",
        F.percent_rank().over(w).alias("is_active_flag_percent_rank"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") < F.col("s.signup_date")), "left_anti")
)

enriched_customers = (
    kept
    .withColumn("is_active_flag_percent_rank_percent_rank", F.percent_rank().over(w3))
    .withColumn("is_active_flag_percent_rank_2", F.percent_rank().over(w4))
)

result = (
    enriched_customers
    .groupBy(
        F.when(F.col("is_active_flag_percent_rank") >= 0.67, "PREMIUM").otherwise("STANDARD").alias("is_active_flag_percent_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit"))).alias("us_total_credit_limit"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy("distinct_ids")
)
