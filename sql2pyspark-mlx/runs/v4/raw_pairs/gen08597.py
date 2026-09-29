from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity_p90").desc())

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 235, "HIGH").otherwise("LOW").alias("quantity_tier"),
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("cnt"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .filter(F.col("avg_quantity") >= 0)
    .withColumn("quantity_p90_percent_rank", F.percent_rank().over(w2))
)
