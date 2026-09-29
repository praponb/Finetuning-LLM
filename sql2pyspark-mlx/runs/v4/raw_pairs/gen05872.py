from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("row_seq", "id")

valid = (
    df_sessions
    .filter(F.col("browser") != "Safari")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

with_windows = (
    scored
    .withColumn("row_seq_2_count_7d", F.count("row_seq_2").over(w3))
    .withColumn("running_row_seq_2", F.sum("row_seq_2").over(w4))
    .withColumn("next_row_seq", F.lead("row_seq", 1, 0).over(w5))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

stats = (
    not_flagged
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("row_seq_2_count_7d", 0.9).alias("row_seq_2_count_7d_p90"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "num_browsers",
        "distinct_browsers",
        "row_seq_2_count_7d_p90",
        "chrome_events",
        "browser_list",
    )
)
