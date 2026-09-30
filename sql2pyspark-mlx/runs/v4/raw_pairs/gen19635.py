from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w_ordered = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("device", "country").orderBy("started_at", "id")
w6 = Window.partitionBy("device").orderBy(F.col("seq_no").desc_nulls_last())
w7 = Window.orderBy(F.col("seq_no").asc_nulls_first())
w8 = Window.partitionBy("customer_id")
w9 = Window.orderBy("seq_no_3")
w10 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w11 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w12 = Window.orderBy(F.col("seq_no_2").asc_nulls_last())

enriched = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_ordered))
    .withColumn("seq_no_2", F.row_number().over(w_running))
    .withColumn("seq_no_3", F.row_number().over(w5))
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.city"), F.col("p.signup_date"), F.col("p.name"))
)

eligible = (
    joined
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([14, 20, 22, 26])), "left_anti")
)

matched_keys = (
    eligible
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["SG", "DE", "JP"]))
        .select("country")
    )
)

verified = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.cume_dist().over(w6).alias("seq_no_cume_dist"),
        F.rank().over(w7).alias("seq_no_rank"),
    )
)

enriched_sessions = (
    metrics
    .withColumn("seq_no_2_share", F.round((F.col("seq_no_2") / F.sum("seq_no_2").over(w8)), 4))
    .withColumn("seq_no_3_dense_rank", F.dense_rank().over(w9))
    .withColumn("seq_no_cume_dist_avg_7d", F.avg("seq_no_cume_dist").over(w10))
    .withColumn("seq_no_avg_30d", F.avg("seq_no").over(w11))
    .withColumn("seq_no_2_percent_rank", F.percent_rank().over(w12))
)

result = (
    enriched_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        "seq_no",
        "row_seq",
        "first_known_browser",
        "seq_no_2",
        "seq_no_3",
        "signup_date",
    )
    .orderBy(F.col("seq_no").asc_nulls_first(), F.col("customer_id").desc())
)
