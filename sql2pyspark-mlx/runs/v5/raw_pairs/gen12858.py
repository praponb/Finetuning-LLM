from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("supplier_id", "brand")

with_windows = (
    df_products
    .filter(F.col("price") > 960)
    .select(
        "id",
        "brand",
        "launch_date",
        "supplier_id",
        F.avg("cost").over(w).alias("cost_avg_6_prior"),
        F.corr("cost", "price").over(w2).alias("corr_cost_price"),
    )
)

result = (
    with_windows
    .cube(
        F.when(F.col("corr_cost_price") >= 0.34, "HIGH").otherwise("LOW").alias("corr_cost_price_tier"),
        F.date_trunc("quarter", "launch_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("cost_avg_6_prior") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("cost_avg_6_prior_per_acme"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.percentile_approx("corr_cost_price", [0.25, 0.5, 0.75]).alias("corr_cost_price_quartiles"),
    )
    .orderBy("corr_cost_price_tier")
)
