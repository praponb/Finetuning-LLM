from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") <= 740), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "new")), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.category"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.length(F.col("x.product_name")) >= 11), "left_anti")
)

result = (
    kept
    .withColumn("cost_tier", F.when(F.col("cost") >= 600, "LARGE").otherwise("SMALL"))
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["country", "cost_tier", "rating_tier"], ["country", "cost_tier"], ["country"]],
        "country",
        "cost_tier",
        "rating_tier",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("rating_per_th"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
