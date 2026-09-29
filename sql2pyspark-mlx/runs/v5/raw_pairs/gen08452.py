from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, -1)

metrics = (
    df_inventory
    .withColumn("next_quantity", F.lead("quantity").over(w_grp))
    .withColumn("prev_quantity", F.lag("quantity").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("quantity") >= 269, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("next_quantity"), 2).alias("avg_next_quantity"),
        F.percentile_approx("quantity_avg_6_prior", 0.95).alias("quantity_avg_6_prior_p95"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
        F.min("prev_quantity").alias("min_prev_quantity"),
    )
    .filter(F.col("min_prev_quantity") >= 0)
)
