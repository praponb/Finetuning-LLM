from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w5 = Window.orderBy(F.col("seq_no").desc_nulls_first())
w6 = Window.orderBy("row_seq")
w_part = Window.partitionBy("customer_id")
w8 = Window.partitionBy("customer_id", "country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

with_windows = (
    df_sessions
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

with_gaps = (
    with_windows
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w4).isNull() | (F.col("started_at") > F.lag("started_at").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w))

remaining_keys = (
    sessions_sessions
    .select("id", "device")
    .intersectAll(
        df_sessions
        .filter(~F.col("browser").isin(["Edge", "Safari", "Chrome"]))
        .select("id", "device")
    )
)

kept_rows = (
    sessions_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w5).alias("seq_no_dense_rank"),
        F.rank().over(w6).alias("row_seq_rank"),
        F.max("session_start_flag").over(w_part).alias("customer_id_max_session_start_flag"),
        F.avg("seq_no").over(w8).alias("seq_no_avg_30d"),
    )
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "last_known_country",
        "seq_no_dense_rank",
        "row_seq_rank",
        "customer_id_max_session_start_flag",
        "seq_no_avg_30d",
        F.col("avg_account_balance"),
    )
)
