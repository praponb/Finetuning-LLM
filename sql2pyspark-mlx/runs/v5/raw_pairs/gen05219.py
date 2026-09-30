from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("latest_rank", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .filter(F.col("order_id").isin([9, 22, 30]))
    .withColumn("unit_price_avg_3_prior", F.avg("unit_price").over(w2))
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w3))
    .withColumn("running_quantity", F.sum("quantity").over(w4))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.stock_qty"), F.col("p.product_name"), F.col("p.category"))
)

enriched_order_items = (
    with_dims
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.price"))
)

valid = (
    enriched_order_items
    .filter(F.col("order_id").isin([12, 15]))
    .withColumn("category_product_name_key", F.concat_ws("/", "category", "product_name"))
    .withColumn("product_name_normalized", F.upper(F.trim("product_name")))
)

result = (
    valid
    .select("id", "product_id", "first_known_unit_price", "unit_price")
    .unpivot(
        ids=["id", "product_id"],
        values=["first_known_unit_price", "unit_price"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
