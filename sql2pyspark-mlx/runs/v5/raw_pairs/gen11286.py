from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, -1)
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isin(["London", "New York"]))
    .select(
        "*",
        F.when(F.col("country").isin(["DE", "TH"]), "Tier 1").otherwise("Other").alias("country_group"),
    )
)

metrics = (
    base
    .withColumn("rating_avg_3_prior", F.avg("rating").over(w))
    .withColumn("cumulative_rating", F.sum("rating").over(w2))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isin([22, 30, 36, 37])), "left_semi")
)

kept_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_anti")
)

not_flagged_suppliers = (
    kept_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.size(F.col("c.tags")) > 1), "left_anti")
)

with_last = (
    not_flagged_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("rating_avg_3_prior") >= 4, "LARGE").otherwise("SMALL").alias("rating_avg_3_prior_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.round(F.avg("rating_avg_3_prior"), 2).alias("avg_rating_avg_3_prior"),
    )
    .orderBy(F.col("avg_rating_avg_3_prior").desc())
)
