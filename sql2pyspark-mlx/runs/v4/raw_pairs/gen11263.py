from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("created_at_dayofweek").asc_nulls_first())
w2 = Window.partitionBy("country").orderBy(F.col("created_at_dayofweek").desc_nulls_first())

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", F.col("elem.price"), F.col("elem.promo"))
)

joined = (
    lines
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.name"), F.col("b.country"))
)

cleaned = (
    joined
    .filter(F.hour("created_at").between(11, 15))
    .select(
        "country",
        F.dayofweek("created_at").alias("created_at_dayofweek"),
        F.date_format("created_at", "yyyy-MM").alias("created_at_label"),
    )
)

base = (
    cleaned
    .filter(~F.col("country").isin(["US", "UK"]))
    .select("*", F.trim("country").alias("clean_country"))
)

enriched = (
    base
    .withColumn("created_at_dayofweek_rank", F.rank().over(w))
    .withColumn("created_at_dayofweek_dense_rank", F.dense_rank().over(w2))
)

filtered = (
    enriched
    .filter(F.col("created_at_dayofweek_rank") != 5)
    .select(
        "country",
        "created_at_dayofweek",
        "created_at_label",
        "clean_country",
        "created_at_dayofweek_dense_rank",
        F.length("country").alias("country_length"),
    )
)

totals = (
    filtered
    .cube(
        F.when(F.col("created_at_dayofweek") >= 67, "LARGE").otherwise("SMALL").alias("created_at_dayofweek_tier"),
        F.when(F.col("country_length") >= 83, "LARGE").otherwise("SMALL").alias("country_length_tier"),
    )
    .agg(
        F.percentile_approx("created_at_dayofweek", 0.5).alias("created_at_dayofweek_p50"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
)

result = (
    totals
    .select(
        "created_at_dayofweek_tier",
        "country_length_tier",
        "created_at_dayofweek_p50",
        "jp_rows",
    )
)
