from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("price_tier").orderBy(F.col("avg_stock_qty").desc())

kept_keys = (
    df_products
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isNotNull())
        .select("supplier_id", "id")
    )
)

verified = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("price") >= 1200, "LARGE").otherwise("SMALL").alias("price_tier"),
        "supplier_id",
    )
    .agg(
        F.avg(F.when(F.col("category") == "Sports", F.col("cost"))).alias("sports_avg_cost"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.count_if(F.col("brand") == "Acme").alias("acme_events"),
    )
    .withColumn("avg_stock_qty_rank", F.rank().over(w))
    .orderBy(F.col("price_tier").desc_nulls_last())
)
