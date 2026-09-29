from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue_per_unit").desc())
w2 = Window.orderBy(F.col("revenue").desc_nulls_last())
w3 = Window.orderBy("revenue_per_unit")

cleaned = (
    df_store_sales
    .filter(F.col("store_id") == 6)
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)

kept_keys = (
    cleaned
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("units") == 14)
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("sale_date") > F.lit("2023-03-11").cast("date")) & (F.col("revenue") > 1400))
    .select(
        "id",
        "sale_date",
        "revenue",
        "sale_date_label",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
        F.when(F.col("revenue") >= 4000, "High").when(F.col("revenue") >= 800, "Medium").otherwise("Low").alias("revenue_tier"),
    )
)

metrics = base.select("*", F.percent_rank().over(w).alias("revenue_per_unit_percent_rank"))

scored = (
    metrics
    .select(
        "id",
        "revenue",
        "sale_date_label",
        "revenue_per_unit",
        "revenue_tier",
        F.cume_dist().over(w2).alias("revenue_cume_dist"),
        F.cume_dist().over(w3).alias("revenue_per_unit_cume_dist"),
    )
)

grouped = (
    scored
    .groupBy(
        F.when(F.col("revenue_per_unit_cume_dist") >= 0.68, "HIGH").otherwise("LOW").alias("revenue_per_unit_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("revenue_cume_dist", [0.25, 0.5, 0.75]).alias("revenue_cume_dist_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "revenue_per_unit_cume_dist_tier",
        "revenue_cume_dist_quartiles",
        "distinct_ids",
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
