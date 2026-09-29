from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("credit_limit", "id")
w3 = Window.partitionBy("segment").orderBy("last3_avg_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

not_flagged = (
    df_customers
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date").between("2023-04-01", "2025-06-11")), "left_anti")
)

not_flagged_customers = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status").isin(["CLOSED", "FROZEN"])), "left_anti")
)

with_windows = (
    not_flagged_customers
    .withColumn("last3_max_age", F.max("age").over(w))
    .withColumn("last3_min_age", F.min("age").over(w))
    .withColumn("last3_avg_age", F.avg("age").over(w))
    .withColumn("prev_age", F.lag("age", 1).over(w2))
)

kept = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.account_type").isin(["CHECKING", "SAVINGS"])), "left_anti")
)

enriched = (
    kept
    .withColumn("cumulative_prev_age", F.sum("prev_age").over(w3))
    .withColumn("credit_limit_bucket", F.ntile(5).over(w4))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser"))
)

result = (
    latest_match
    .groupBy(F.date_trunc("month", "signup_date").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("last3_avg_age")).otherwise(0)).alias("chrome_last3_avg_age"),
    )
    .filter(F.col("chrome_last3_avg_age") >= 0)
)
