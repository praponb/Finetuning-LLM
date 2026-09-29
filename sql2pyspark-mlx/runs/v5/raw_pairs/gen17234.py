from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_rounded").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("quantity").between(4, 5))
    .select("*", F.round("unit_price", 0).alias("unit_price_rounded"))
)

with_dims = (
    base
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.brand"), F.col("p.product_name"))
)

deduped = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

valid = (
    deduped
    .filter(F.col("order_id").isin([16, 31]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_rounded",
        "brand",
        "product_name",
        "row_num",
        F.least("row_num", "unit_price_rounded").alias("least_row_num_unit_price_rounded"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.*", F.col("p.brand").alias("product_brand"))
)

scored = (
    enriched
    .withColumn("running_max_least_row_num_unit_price_rounded", F.max("least_row_num_unit_price_rounded").over(w2))
    .withColumn("running_sum_least_row_num_unit_price_rounded", F.sum("least_row_num_unit_price_rounded").over(w2))
    .withColumn("running_count_least_row_num_unit_price_rounded", F.count("least_row_num_unit_price_rounded").over(w2))
)

metrics = (
    scored
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_rounded",
        "brand",
        "product_name",
        "row_num",
        "running_max_least_row_num_unit_price_rounded",
        "running_count_least_row_num_unit_price_rounded",
        F.max("running_sum_least_row_num_unit_price_rounded").over(w3).alias("running_max_running_sum_least_row_num_unit_price_rounded"),
        F.count("running_sum_least_row_num_unit_price_rounded").over(w3).alias("running_count_running_sum_least_row_num_unit_price_rounded"),
        F.avg("running_sum_least_row_num_unit_price_rounded").over(w3).alias("running_avg_running_sum_least_row_num_unit_price_rounded"),
        F.sum("running_sum_least_row_num_unit_price_rounded").over(w3).alias("running_sum_running_sum_least_row_num_unit_price_rounded"),
    )
)

result = (
    metrics
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .groupingSets(
        [["product_brand", "order_item_brand"], ["product_brand"], []],
        F.col("d.brand").alias("product_brand"),
        F.col("src.brand").alias("order_item_brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.running_sum_running_sum_least_row_num_unit_price_rounded")), 2).alias("avg_running_sum_running_sum_least_row_num_unit_price_rounded"),
        F.avg(F.col("src.running_count_least_row_num_unit_price_rounded")).alias("avg_running_count_least_row_num_unit_price_rounded"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
