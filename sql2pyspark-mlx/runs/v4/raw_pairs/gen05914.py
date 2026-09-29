from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .cube("promo")
    .agg(
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.count("*").alias("cnt"),
        F.percentile_approx("pos", 0.5).alias("pos_p50"),
    )
    .filter(F.col("pos_p50") < 0)
    .orderBy(F.col("promo").desc_nulls_first())
)
