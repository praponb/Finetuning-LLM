from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city", "status").orderBy("credit_limit", "id")
w2 = Window.partitionBy("city").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("city", "customer_id").orderBy(F.col("start_date").desc_nulls_first(), "id")

joined = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.credit_limit"), F.col("c.age"), F.col("c.city"))
)

metrics = (
    joined
    .select(
        "*",
        F.row_number().over(w_running).alias("row_seq"),
        F.lag("credit_limit", 1).over(w2).alias("prev_credit_limit"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.start_date",
        "a.credit_limit",
        "a.city",
        F.col("b.segment"),
    )
)

deduped = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    deduped
    .filter((F.col("start_date") > F.lit("2023-02-26").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "customer_id",
        "start_date",
        "city",
        "segment",
        "dedup_rank",
        F.concat_ws("-", "city", "segment").alias("city_segment_key"),
    )
)

result = (
    valid
    .groupBy("city", "segment")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("segment")).alias("num_segments_2"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("num_segments") > 1)
    .orderBy(F.col("segment").desc_nulls_first())
)
