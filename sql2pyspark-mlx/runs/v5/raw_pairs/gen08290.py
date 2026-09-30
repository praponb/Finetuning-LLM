from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("line_total").desc_nulls_first(), "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([3, 5, 29]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total_2"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.unit_price",
        "s.line_total",
        F.col("b.brand"),
        F.col("b.cost"),
        F.col("b.category"),
    )
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    ranked
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(130, 180))
        .select("product_id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("quantity") >= 4)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        "brand",
        "latest_rank",
        F.round((F.col("line_total") / F.col("quantity")), 2).alias("line_total_per_quantity"),
    )
)

valid_order_items = (
    filtered
    .filter(F.col("line_total_per_quantity") < 38)
    .select(
        "*",
        F.round((F.col("latest_rank") * 0.85), 2).alias("latest_rank_scaled"),
        (F.col("unit_price") + F.col("line_total")).alias("unit_price_plus_line_total"),
    )
)

daily = (
    valid_order_items
    .groupBy(
        "product_id",
        F.when(F.col("line_total") >= 74, "LARGE").otherwise("SMALL").alias("line_total_tier"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("line_total")).otherwise(0)).alias("globex_line_total"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sort_array(F.collect_set("brand")).alias("brand_list_2"),
        F.round(F.sum("latest_rank_scaled") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("latest_rank_scaled_per_umbrella"),
    )
    .filter(F.col("latest_rank_scaled_per_umbrella") > 0)
)

result = (
    daily
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "line_total_tier",
        "unit_price_quartiles",
        "num_brands",
        "brand_list",
        "brand_list_2",
        "latest_rank_scaled_per_umbrella",
        F.col("avg_order_item_unit_price"),
    )
)
