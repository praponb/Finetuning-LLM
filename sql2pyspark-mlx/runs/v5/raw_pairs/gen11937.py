from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("prev_quantity_adjusted").asc_nulls_last(), "id")

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 5, 10]))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 0.85), 2))
)

valid = (
    cleaned
    .filter(F.col("last_restock") >= F.lit("2023-06-08").cast("date"))
    .select(
        "*",
        F.least("quantity", "quantity_adjusted").alias("least_quantity_quantity_adjusted"),
    )
)

metrics = (
    valid
    .filter(F.col("least_quantity_quantity_adjusted") < 48)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "quantity_adjusted",
        F.lag("least_quantity_quantity_adjusted").over(w_hist).alias("prev_least_quantity_quantity_adjusted"),
        F.lead("quantity_adjusted").over(w_hist).alias("next_quantity_adjusted"),
        F.lag("quantity_adjusted").over(w_hist).alias("prev_quantity_adjusted"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "last_restock", "quantity_adjusted", "last_known_quantity")
)

stats = (
    ranked
    .cube(
        F.date_trunc("year", "last_restock").alias("year_start"),
        F.when(F.col("quantity_adjusted") >= 82, "PREMIUM").otherwise("STANDARD").alias("quantity_adjusted_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("last_known_quantity"), 2).alias("avg_last_known_quantity"),
        F.sum("quantity_adjusted").alias("total_quantity_adjusted"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "quantity_adjusted_tier",
        "cnt",
        "avg_last_known_quantity",
        "total_quantity_adjusted",
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
