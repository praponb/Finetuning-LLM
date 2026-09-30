from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-05-08")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        F.date_format("launch_date", "yyyy-MM").alias("launch_date_label"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= "2024-12-01"), "left_anti")
)

deduped = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") == 3), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "brand",
        F.col("price").alias("PRICE_VAL"),
        F.col("cost").alias("COST"),
    )
    .unpivot(
        ids=["id", "brand"],
        values=["PRICE_VAL", "COST"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
