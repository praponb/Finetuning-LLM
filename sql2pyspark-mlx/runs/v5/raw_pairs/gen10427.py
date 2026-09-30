from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc_nulls_first())
w2 = Window.orderBy(F.col("quantity_filled").desc(), "id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_filled_bucket").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-09-05").cast("date"))
    .select(
        "id",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

metrics = (
    valid
    .filter(F.col("product_id").isin([4, 9, 10]))
    .select(
        "id",
        "product_id",
        F.rank().over(w).alias("quantity_filled_rank"),
        F.ntile(4).over(w2).alias("quantity_filled_bucket"),
        F.min("quantity_filled").over(w3).alias("product_id_min_quantity_filled"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "quantity_filled_bucket")
)

joined = (
    ranked
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity_filled_bucket",
        F.col("d.launch_date"),
        F.col("d.cost"),
        F.col("d.price"),
    )
)

result = (
    joined
    .select("id", "product_id", "price", "cost")
    .unpivot(
        ids=["id", "product_id"],
        values=["price", "cost"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
