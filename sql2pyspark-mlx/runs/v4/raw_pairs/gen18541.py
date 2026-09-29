from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("device", "browser").orderBy(F.col("started_at").desc(), "id")

valid = (
    df_sessions
    .filter(F.col("started_at") > F.to_date(F.lit("2023-11-26")))
    .select("*", F.lower("browser").alias("browser_lower"))
)

with_prev = valid.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

enriched = (
    islands
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w3))
    .withColumn("new_group_flag_sum_7d", F.sum("new_group_flag").over(w4))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "browser_lower",
        "prev_started_at",
        "session_no",
        "last_known_country",
    )
)
