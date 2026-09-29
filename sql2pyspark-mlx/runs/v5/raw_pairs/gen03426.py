from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("segment").orderBy("age_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("city_avg_credit_limit", "id")
w5 = Window.orderBy("city_avg_credit_limit", "id")
w6 = Window.orderBy("running_count_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_max_age", "id")

filtered = (
    df_customers
    .filter(F.col("signup_date") > "2023-08-01")
    .select("*", F.upper("segment").alias("segment_upper"))
)

with_windows = (
    filtered
    .filter(F.col("city").isin(["Berlin", "Tokyo"]))
    .select(
        "id",
        "email",
        "segment",
        "age",
        F.ntile(3).over(w).alias("age_bucket"),
        F.avg("credit_limit").over(w2).alias("city_avg_credit_limit"),
    )
)

with_windows_customers = (
    with_windows
    .select(
        "id",
        "age",
        "city_avg_credit_limit",
        F.min("age").over(w3).alias("running_min_age"),
        F.avg("age").over(w3).alias("running_avg_age"),
        F.count("age").over(w3).alias("running_count_age"),
        F.max("age").over(w3).alias("running_max_age"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

enriched = (
    with_windows_customers
    .select(
        "id",
        "age",
        "running_avg_age",
        "running_count_age",
        "running_max_age",
        F.row_number().over(w5).alias("seq_no"),
        F.sum("city_avg_credit_limit").over(w6).alias("running_city_avg_credit_limit"),
        F.ntile(4).over(w7).alias("running_max_age_bucket"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isNotNull()), "left_anti")
)

eligible_customers = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isNotNull()), "left_semi")
)

remaining_keys = (
    eligible_customers
    .select("id")
    .subtract(
        df_customers
        .filter(F.month("signup_date").isin([7, 8, 10]))
        .select("id")
    )
)

reconciled = (
    eligible_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "age",
        "running_avg_age",
        "running_count_age",
        "running_max_age",
        "seq_no",
        "running_city_avg_credit_limit",
        "running_max_age_bucket",
    )
    .orderBy(F.col("running_max_age_bucket").asc_nulls_last())
)
