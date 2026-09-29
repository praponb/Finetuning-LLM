from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc())
w3 = Window.orderBy(F.col("rating").asc_nulls_first())
w4 = Window.orderBy(F.col("running_sum_rating").asc_nulls_first())

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.upper(F.trim(F.col("ch.product_name"))) == "OFFICE CHAIR"), "left_anti")
)

valid = (
    eligible
    .filter(F.col("city").isin(["Chiang Mai", "New York", "Bangkok"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "supplier_name",
        "rating",
        F.min("rating").over(w_part).alias("running_min_rating"),
        F.count("rating").over(w_part).alias("running_count_rating"),
        F.sum("rating").over(w_part).alias("running_sum_rating"),
        F.percent_rank().over(w2).alias("rating_percent_rank"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .exceptAll(
        df_suppliers
        .filter(F.col("supplier_name").like("Pa%"))
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("id")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").like("Eur%"))
        .select("id")
    )
)

reconciled_suppliers = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible_suppliers = (
    reconciled_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "premium")), "left_anti")
)

with_windows_suppliers = (
    eligible_suppliers
    .select(
        "rating",
        "running_count_rating",
        "running_sum_rating",
        "rating_percent_rank",
        F.percent_rank().over(w3).alias("rating_percent_rank_2"),
        F.rank().over(w4).alias("running_sum_rating_rank"),
    )
)

result = (
    with_windows_suppliers
    .groupBy(
        F.when(F.col("running_count_rating") >= 15, "HIGH").otherwise("LOW").alias("running_count_rating_tier"),
    )
    .agg(
        F.percentile_approx("rating_percent_rank_2", [0.25, 0.5, 0.75]).alias("rating_percent_rank_2_quartiles"),
        F.percentile_approx("running_count_rating", 0.5).alias("running_count_rating_p50"),
    )
    .orderBy("running_count_rating_tier")
)
