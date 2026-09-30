from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("age", "id")
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("age").desc(), "id")

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-08-01").cast("date")) & F.col("status").isin(["EXPIRED", "PAUSED"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("s.id", "s.customer_id", "s.start_date", "s.monthly_fee", F.col("b.age"))
)

enriched = (
    joined
    .select(
        "id",
        "customer_id",
        "start_date",
        "age",
        F.ntile(3).over(w).alias("age_bucket"),
        F.last("age", ignorenulls=True).over(w2).alias("last_known_age"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

joined_subscriptions = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.credit_limit") > 10300)
    .select("s.*", F.col("d.city"))
)

base = (
    joined_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-07-12"))) & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "start_date",
        "age",
        "age_bucket",
        "last_known_age",
        "event_seq",
        "city",
        F.round("last_known_age", 0).alias("last_known_age_rounded"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    base
    .select("id", "start_date", "age", "event_seq")
    .orderBy(F.col("event_seq").asc_nulls_last())
)
