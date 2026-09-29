from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"), F.col("c.stock_qty"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("stock_qty") >= 312, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("product_category") == "Grocery", 1)), F.lit(0)), 2).alias("stock_qty_per_grocery"),
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .filter(F.col("rating_p50") > 0)
    .orderBy(F.col("rating_p50").desc())
)
