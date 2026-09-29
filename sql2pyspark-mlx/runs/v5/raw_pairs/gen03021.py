from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w_recent = Window.partitionBy("country", "city").orderBy(F.col("cost").desc())
w4 = Window.partitionBy("country")
w_ordered = Window.orderBy(F.col("cost_percent_rank").desc())
w6 = Window.orderBy("rating")
w7 = Window.partitionBy("city")
w8 = Window.orderBy(F.col("rating").asc_nulls_first())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

latest = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

remaining_keys = (
    latest
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["JP", "SG"]))
        .select("id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("city") == "London")
    .withColumn("cost_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w4)), 4))
    .withColumn("country_count_cost", F.count("cost").over(w4))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
)

matched_keys = (
    metrics
    .select("id", "country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") > 3.9)
        .select("id", "country")
    )
)

kept_rows_suppliers = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

metrics_suppliers = (
    kept_rows_suppliers
    .select(
        "supplier_name",
        "country",
        "city",
        "rating",
        "country_count_cost",
        F.rank().over(w_ordered).alias("cost_percent_rank_rank"),
        F.percent_rank().over(w6).alias("rating_percent_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w7)), 4).alias("rating_share_2"),
        F.dense_rank().over(w8).alias("rating_dense_rank"),
    )
)

remaining_keys_2 = (
    metrics_suppliers
    .select("city", "country")
    .exceptAll(
        df_suppliers
        .filter(F.col("city") == "Bangkok")
        .select("city", "country")
    )
)

reconciled = (
    metrics_suppliers
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.percentile_approx("cost_percent_rank_rank", [0.25, 0.5, 0.75]).alias("cost_percent_rank_rank_quartiles"),
        F.countDistinct("city").alias("distinct_cities"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
    .orderBy("distinct_countries")
)
