from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

deduped = (
    df_inventory
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
        "store_id",
        "product_id",
    )
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50_2"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("quantity_p50").desc_nulls_first())
)
