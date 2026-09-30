from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")
w_grp = Window.partitionBy("session_browser")
w_part = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("session_browser").orderBy(F.col("session_browser_avg_credit_limit").desc_nulls_first())
w_part_2 = Window.partitionBy("session_browser").orderBy("started_at", "id")
w_running = Window.partitionBy("session_browser").orderBy(F.col("credit_limit").asc_nulls_last())

cleaned = (
    df_customers
    .filter((F.col("city") == "New York") & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.concat("name", F.lit(" - "), "city").alias("name_city"),
    )
)

cleaned_customers = (
    cleaned
    .filter(~F.col("is_active"))
    .withColumn("segment_name_key", F.concat_ws("/", "segment", "name"))
)

latest_match = (
    cleaned_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.credit_limit",
        "s.name_city",
        F.col("c.started_at"),
        F.col("c.browser").alias("session_browser"),
    )
)

scored = (
    latest_match
    .select(
        "*",
        F.ntile(4).over(w2).alias("credit_limit_bucket"),
        F.min("credit_limit").over(w_grp).alias("session_browser_min_credit_limit"),
        F.avg("credit_limit").over(w_grp).alias("session_browser_avg_credit_limit"),
    )
)

with_windows = (
    scored
    .withColumn("session_browser_avg_credit_limit_max_90d", F.max("session_browser_avg_credit_limit").over(w_part))
    .withColumn("session_browser_avg_credit_limit_cume_dist", F.cume_dist().over(w5))
    .withColumn("prev_credit_limit", F.lag("credit_limit", 1, 0).over(w_part_2))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w_running))
)

result = (
    with_windows
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.sum("prev_credit_limit").alias("total_prev_credit_limit"),
        F.count(F.when(F.col("session_browser") == "Edge", 1)).alias("edge_rows"),
    )
    .orderBy(F.col("week_start").desc_nulls_first())
)
