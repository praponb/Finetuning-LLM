from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

with_dims = (
    lines
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        F.col("c.signup_date"),
        F.col("c.country"),
        F.col("c.city"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.customer_id",
        "src.signup_date",
        "src.country",
        F.col("p.city").alias("customer_city"),
    )
)

valid = (
    joined
    .filter(~F.col("customer_city").eqNullSafe(F.lit("Bangkok")))
    .withColumn("signup_date_month_end", F.last_day("signup_date"))
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.customer_id", "s.signup_date", "s.customer_city", F.col("b.segment"))
)

totals = (
    enriched
    .cube("customer_id")
    .agg(
        F.sort_array(F.collect_set("customer_city")).alias("customer_city_list"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.sort_array(F.collect_set("customer_city")).alias("customer_city_list_2"),
        F.sort_array(F.collect_set("customer_city")).alias("customer_city_list_3"),
    )
)

result = (
    totals
    .select(
        "customer_id",
        "customer_city_list",
        "event_count",
        "corporate_rows",
        "customer_city_list_2",
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
