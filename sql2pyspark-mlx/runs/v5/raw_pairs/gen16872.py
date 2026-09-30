from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-12-01")
    .withColumn("quantity_str", F.col("quantity").cast("string"))
)

cleaned = (
    filtered
    .filter((F.col("last_restock") >= F.lit("2023-04-27").cast("date")) & (F.col("quantity") < 237))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

cleaned_inventory = (
    cleaned
    .filter(F.col("last_restock") > F.lit("2023-02-05").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_str",
        "quantity_filled",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled_2"),
    )
)

base = (
    cleaned_inventory
    .filter(F.col("last_restock") > F.lit("2023-02-05").cast("date"))
    .select(
        "*",
        (F.col("quantity") + F.col("quantity_filled")).alias("quantity_plus_quantity_filled"),
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

summary = (
    latest
    .rollup("store_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("quantity_plus_quantity_filled", [0.25, 0.5, 0.75]).alias("quantity_plus_quantity_filled_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity_plus_quantity_filled", 0.5).alias("quantity_plus_quantity_filled_p50"),
        F.grouping("store_id").alias("grouping_level"),
    )
)

result = (
    summary
    .select(
        "num_rows",
        "quantity_plus_quantity_filled_quartiles",
        "total_quantity",
        "avg_quantity",
        "grouping_level",
    )
    .orderBy(F.col("avg_quantity").desc())
)
