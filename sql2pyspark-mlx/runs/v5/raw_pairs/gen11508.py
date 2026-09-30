from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.rating",
        F.col("c.cost"),
        F.col("c.stock_qty"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("rating") >= 3.8, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("stock_qty").alias("max_stock_qty"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.max("stock_qty").alias("max_stock_qty_2"),
    )
    .filter(F.col("max_stock_qty_2") >= 0)
)
