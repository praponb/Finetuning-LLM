from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.signup_date"), F.col("d.segment"), F.col("d.age"))
)

scored = with_dims.select("*", F.avg("age").over(w).alias("age_avg_2h"))

with_dims_carts = (
    scored
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("e.*", F.col("b.signup_date").alias("customer_signup_date"))
)

enriched = (
    with_dims_carts
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.city"), F.col("p.country"))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "customer_signup_date").alias("quarter_start"),
        F.when(F.col("age_avg_2h") >= 61, "LARGE").otherwise("SMALL").alias("age_avg_2h_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.avg(F.when(F.col("country") == "JP", F.col("age_avg_2h"))).alias("jp_avg_age_avg_2h"),
        F.min("created_at").alias("started_at"),
        F.sum(F.when(F.col("key") == "gift", F.col("age_avg_2h")).otherwise(0)).alias("gift_age_avg_2h"),
    )
    .filter(F.col("gift_age_avg_2h") > 0)
    .orderBy(F.col("jp_avg_age_avg_2h").asc_nulls_last())
)
