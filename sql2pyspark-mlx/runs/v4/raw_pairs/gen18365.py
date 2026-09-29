from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w4 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w_grp = Window.orderBy("age", "id")
w7 = Window.partitionBy("plan").orderBy("running_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_customers
    .select(
        "*",
        F.sum("credit_limit").over(w_ordered).alias("cumulative_credit_limit"),
        F.sum("age").over(w2).alias("running_age"),
        F.ntile(10).over(w3).alias("credit_limit_bucket"),
        F.first("city", ignorenulls=True).over(w4).alias("first_known_city"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") >= F.col("s.signup_date")), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.plan"),
        F.col("c.start_date").alias("subscription_start_date"),
    )
)

filtered = (
    with_last
    .filter(F.col("plan").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "age",
        "cumulative_credit_limit",
        "running_age",
        "plan",
        F.when(F.col("age") >= 73, "High").when(F.col("age") >= 33, "Medium").otherwise("Low").alias("age_tier"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.monthly_fee") <= 240), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("running_age").between(198, 285))
    .select(
        "*",
        F.round((F.col("credit_limit") * 0.9), 2).alias("adjusted_credit_limit"),
        F.round("cumulative_credit_limit", 1).alias("cumulative_credit_limit_rounded"),
    )
)

result = (
    base
    .withColumn("prev_credit_limit", F.lag("credit_limit", 1, 0).over(w_grp))
    .withColumn("cumulative_cumulative_credit_limit_rounded", F.sum("cumulative_credit_limit_rounded").over(w7))
)
