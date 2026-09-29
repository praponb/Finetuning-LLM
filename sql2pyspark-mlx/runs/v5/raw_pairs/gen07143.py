from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").asc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last(), "id")
w3 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("row_num", "id")
w6 = Window.orderBy("first_known_credit_limit", "id")
w_recent = Window.orderBy("prev_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-03-09"))) & (~F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "credit_limit",
        F.concat_ws("/", "segment", "name").alias("segment_name_key"),
        F.substring("city", 1, 2).alias("city_prefix"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped_customers = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "name",
        "email",
        "credit_limit",
        "segment_name_key",
        "city_prefix",
        "row_num",
    )
)

kept = (
    deduped_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status") == "CANCELLED"), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (~F.col("c.browser").isin(["Edge", "Chrome", "Safari"])), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.sum("credit_limit").over(w3).alias("running_credit_limit"),
        F.first("credit_limit", ignorenulls=True).over(w4).alias("first_known_credit_limit"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "email",
        "credit_limit",
        "running_credit_limit",
        "first_known_credit_limit",
        F.lag("credit_limit", 2).over(w5).alias("prev_credit_limit"),
        F.lead("running_credit_limit", 1).over(w6).alias("next_running_credit_limit"),
    )
)

result = (
    scored
    .filter(F.col("first_known_credit_limit") >= 13800)
    .select(
        "*",
        F.sum("credit_limit").over(w_recent).alias("cumulative_credit_limit"),
        F.last("credit_limit", ignorenulls=True).over(w_recent).alias("last_known_credit_limit"),
    )
)
