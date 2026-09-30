from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w4 = Window.orderBy("last_known_credit_limit", "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("country").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-19").cast("date")) & F.col("segment").isNotNull())
    .select("*", F.col("is_active").cast("int").alias("is_active_flag"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_anti")
)

with_windows = (
    eligible
    .filter(F.col("credit_limit").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "credit_limit",
        "is_active_flag",
        F.last("credit_limit", ignorenulls=True).over(w).alias("last_known_credit_limit"),
        F.sum("credit_limit").over(w2).alias("cumulative_credit_limit"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.balance").isNotNull()), "left_anti")
)

scored = (
    not_flagged
    .select(
        "id",
        "name",
        "country",
        "credit_limit",
        "is_active_flag",
        "cumulative_credit_limit",
        F.ntile(5).over(w4).alias("last_known_credit_limit_bucket"),
        F.max("last_known_credit_limit").over(w5).alias("city_max_last_known_credit_limit"),
        F.sum("cumulative_credit_limit").over(w6).alias("cumulative_cumulative_credit_limit"),
    )
)

result = (
    scored
    .select(
        "id",
        "credit_limit",
        "cumulative_credit_limit",
        "last_known_credit_limit_bucket",
        "city_max_last_known_credit_limit",
    )
)
