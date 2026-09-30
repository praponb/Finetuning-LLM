from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("price_tier").orderBy(F.col("acme_events").desc())

kept = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 7])), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("price") >= 920, "HIGH").otherwise("LOW").alias("price_tier"),
        F.date_trunc("quarter", "launch_date").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count_if(F.col("brand") == "Acme").alias("acme_events"),
    )
    .withColumn("acme_events_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("quarter_start").desc())
)
