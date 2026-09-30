from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("created_at") < "2024-05-12")
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("bulk_lines") >= 3, "LARGE").otherwise("SMALL").alias("bulk_lines_tier"),
    )
    .agg(
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50"),
        F.sum("items_total").alias("total_items_total"),
        F.percentile_approx("items_total", [0.25, 0.5, 0.75]).alias("items_total_quartiles"),
    )
    .filter(F.col("bulk_lines_p50") > 0)
    .orderBy(F.col("bulk_lines_p50").desc_nulls_first())
)
