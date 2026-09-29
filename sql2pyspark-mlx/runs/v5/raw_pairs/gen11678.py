from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("age").asc_nulls_last())
w2 = Window.partitionBy("customer_id", "element").orderBy(F.col("age").asc_nulls_last())

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

filtered = (
    exploded
    .filter(F.col("element").isNotNull() & (F.col("customer_id") == 29))
    .select("*", F.upper("element").alias("element_upper"))
)

enriched = (
    filtered
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.name"), F.col("p.age"), F.col("p.country"))
)

scored = (
    enriched
    .filter(F.col("customer_id").isin([7, 20, 24, 25]))
    .select(
        "*",
        F.percent_rank().over(w).alias("age_percent_rank"),
        F.percent_rank().over(w2).alias("age_percent_rank_2"),
    )
)

kept_keys = (
    scored
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2024-02-01")
        .select("id", "customer_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.city").alias("city"), F.col("dim.segment").alias("segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.age_percent_rank")).alias("avg_age_percent_rank"),
    )
)
