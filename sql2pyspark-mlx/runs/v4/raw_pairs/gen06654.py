from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country")
w2 = Window.partitionBy("segment")
w_part = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-10-24").cast("date")) & (F.col("country") == "DE"))
    .withColumn("email_filled", F.coalesce("email", F.lit("N/A")))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("DIRECT")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date").between("2024-02-01", "2024-06-03")), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.ntile(4).over(w_part).alias("age_bucket"),
        F.dense_rank().over(w4).alias("credit_limit_dense_rank"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .withColumn("credit_limit_dense_rank_tier", F.when(F.col("credit_limit_dense_rank") >= 8, "LARGE").otherwise("SMALL"))
    .rollup("credit_limit_dense_rank_tier")
    .agg(
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.grouping_id().alias("grouping_id"),
    )
)
