from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"), F.col("c.launch_date"))
)

result = (
    with_last
    .rollup("product_brand", F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.size(F.collect_set("product_brand")).alias("num_product_brands"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("rating_per_sg"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating_2"),
        F.round(F.avg("rating"), 2).alias("avg_rating_3"),
    )
    .orderBy(F.col("week_start").desc())
)
