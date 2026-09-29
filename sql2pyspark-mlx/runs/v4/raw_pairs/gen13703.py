from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").asc_nulls_first())
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w3 = Window.orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w5 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy("started_at", "id")
w7 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_sessions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
)

enriched = (
    scored
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email"))
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.name"), F.col("c.signup_date"))
)

gaps = (
    joined
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w5))

with_gaps = (
    sessionized
    .withColumn("is_new_burst_2", F.when(F.lag("started_at").over(w6).isNull() | (F.col("started_at") > F.lag("started_at").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_burst_2").over(w7))

with_dims = (
    islands
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.credit_limit"),
        F.col("d.email").alias("customer_email"),
        F.col("d.signup_date").alias("customer_signup_date"),
    )
)

result = (
    with_dims
    .groupBy("session_id")
    .agg(
        F.min("started_at").alias("first_seen"),
        F.avg(F.when(F.col("browser") == "Safari", F.col("burst_id"))).alias("safari_avg_burst_id"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
    )
    .filter(F.col("edge_events") > 1)
    .orderBy("session_id")
)
