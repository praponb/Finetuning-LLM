from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("country").orderBy("amount", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("first_known_amount").desc(), "id")
w6 = Window.orderBy("amount_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("age", "id").rowsBetween(-5, -1)

base = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("signup_date") > "2024-10-01"))
    .select(
        "*",
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "Priority").otherwise("Other").alias("segment_group"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.is_active",
        "s.age",
        "s.segment_group",
        F.col("c.amount"),
    )
)

latest = (
    with_last
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") <= "2023-06-01"), "left_anti")
)

scored = (
    kept
    .filter(F.col("age").between(47, 50))
    .select(
        "id",
        "name",
        "age",
        "segment_group",
        F.avg("amount").over(w3).alias("amount_avg_6_prior"),
        F.first("amount", ignorenulls=True).over(w4).alias("first_known_amount"),
    )
)

scored_customers = (
    scored
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("first_known_amount_avg_6_prior", F.first("amount_avg_6_prior", ignorenulls=True).over(w6))
    .withColumn("first_known_amount_bucket", F.ntile(4).over(w_hist))
    .withColumn("amount_avg_6_prior_avg_5_prior", F.avg("amount_avg_6_prior").over(w7))
)

base_customers = (
    scored_customers
    .filter(F.col("first_known_amount") >= 430)
    .select(
        "*",
        F.round((F.col("row_seq") / F.col("amount_avg_6_prior_avg_5_prior")), 2).alias("row_seq_per_amount_avg_6_prior_avg_5_prior"),
    )
)

result = (
    base_customers
    .groupBy(
        F.when(F.col("first_known_amount_bucket") >= 9, "HIGH").otherwise("LOW").alias("first_known_amount_bucket_tier"),
    )
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
