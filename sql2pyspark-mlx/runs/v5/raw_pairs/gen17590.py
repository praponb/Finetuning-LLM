from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "city").orderBy("stock_qty", "id")
w2 = Window.partitionBy("city", "brand").orderBy("launch_date", "id")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

valid = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Acme")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.size("tags").alias("tags_count"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.city"))
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.lead("tags_count").over(w2).alias("next_tags_count"),
        F.lag("cost").over(w2).alias("prev_cost"),
        F.lead("cost").over(w2).alias("next_cost"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("stock_qty").over(w3).alias("city_avg_stock_qty"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    with_last
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("brand").rlike("^(Globex|Umbrella)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("brand_zone"),
        F.col("min_product_cost"),
    )
    .orderBy("launch_date", "min_product_cost")
)
