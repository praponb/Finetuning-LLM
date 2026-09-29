from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_first(), "id")
w2 = Window.orderBy(F.col("age").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_last())
w4 = Window.orderBy(F.col("credit_limit").asc_nulls_first())
w5 = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_first())
w6 = Window.orderBy("credit_limit")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-05-26"))) & F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .select(
        "id",
        "name",
        "email",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.date_add("signup_date", 25).alias("signup_date_plus_25d"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    ranked
    .select("id", "city")
    .intersectAll(
        df_customers
        .filter(F.length("name") > 12)
        .select("id", "city")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date_plus_25d")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.cume_dist().over(w2).alias("age_cume_dist"),
        F.percent_rank().over(w3).alias("age_percent_rank"),
        F.percent_rank().over(w4).alias("credit_limit_percent_rank"),
        F.cume_dist().over(w5).alias("age_cume_dist_2"),
        F.dense_rank().over(w6).alias("credit_limit_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("QUARTER", "signup_date_plus_25d").alias("quarter_start"),
        "city",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)
