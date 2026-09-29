from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

valid = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.country").isin(["JP", "UK", "SG"])), "left_anti")
)

deduped = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

eligible_customers = (
    deduped
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.signup_date")), "left_anti")
)

not_flagged = (
    eligible_customers
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < F.col("s.signup_date")), "left_semi")
)

latest = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("country", "segment")
    .agg(
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("age_per_consumer"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.sum("age").alias("total_age"),
        F.countDistinct("country").alias("distinct_countries"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("distinct_countries") < 1)
    .orderBy(F.col("num_countries").asc_nulls_first())
)
