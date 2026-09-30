from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

with_dims = (
    exploded
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.name").like("%Kanya%"))
    .select("s.customer_id", "s.created_at", F.col("p.age"))
)

cleaned = (
    with_dims
    .filter(F.col("customer_id").isin([20, 21, 39]))
    .withColumn("age_num", F.col("age").cast("double"))
)

with_dims_carts = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.customer_id", "s.created_at", F.col("c.segment"))
)

filtered = (
    with_dims_carts
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.dayofweek("created_at").alias("created_at_dayofweek"),
        F.col("created_at").cast("date").alias("created_at_day"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.name"), F.col("d.signup_date"), F.col("d.credit_limit"))
)

enriched_carts = (
    enriched
    .select(
        "created_at_day",
        "resolved_segment",
        "name",
        "signup_date",
        "credit_limit",
        F.max("credit_limit").over(w).alias("credit_limit_max_2h"),
    )
)

result = (
    enriched_carts
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.sum("credit_limit_max_2h").alias("total_credit_limit_max_2h"),
        F.round(F.avg("credit_limit_max_2h"), 2).alias("avg_credit_limit_max_2h"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .orderBy("total_credit_limit_max_2h")
)
