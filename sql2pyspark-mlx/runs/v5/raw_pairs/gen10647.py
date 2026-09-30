from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price_filled").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy("quantity", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity_avg_3_prior").asc_nulls_last(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

filtered = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .filter(F.col("product_id").isNull())
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w2))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w3))
    .withColumn("order_id_avg_unit_price", F.avg("unit_price").over(w_part))
)

enriched = (
    scored
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_filled",
        "row_num",
        "quantity_avg_3_prior",
        "order_id_avg_unit_price",
        F.ntile(4).over(w5).alias("unit_price_bucket"),
        F.lead("order_id_avg_unit_price", 1, 0).over(w6).alias("next_order_id_avg_unit_price"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_filled",
        "row_num",
        "quantity_avg_3_prior",
        "order_id_avg_unit_price",
        "unit_price_bucket",
        F.col("min_store_sale_units"),
    )
    .orderBy(F.col("quantity").desc_nulls_first())
)
