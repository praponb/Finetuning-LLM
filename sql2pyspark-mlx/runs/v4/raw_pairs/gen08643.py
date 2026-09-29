from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("session_no").desc())
w4 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("started_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy(F.col("new_group_flag").desc_nulls_last())
w7 = Window.partitionBy("country").orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at_date").desc_nulls_first())

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-12-31"))) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.to_date("started_at").alias("started_at_date"),
    )
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

metrics = (
    islands
    .select(
        "*",
        F.percent_rank().over(w3).alias("session_no_percent_rank"),
        F.sum("session_no").over(w4).alias("session_no_sum_90d"),
        F.sum("new_group_flag").over(w5).alias("running_new_group_flag"),
        F.dense_rank().over(w6).alias("new_group_flag_dense_rank"),
        F.sum("session_no").over(w7).alias("cumulative_session_no"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.email"), F.col("d.country").alias("customer_country"))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "started_at_date",
        "new_group_flag",
        "session_no",
    )
    .orderBy("started_at_date")
)
