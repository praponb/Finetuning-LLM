from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("rn").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

ranked = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select("id", "order_id", "product_id", "quantity", "unit_price", "rn")
)

enriched = (
    deduped
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNull())
    .select("src.*", F.col("p.cost"), F.col("p.brand"), F.col("p.price"))
)

remaining_keys = (
    enriched
    .select("product_id", "id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price") >= 200)
        .select("product_id", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_order_items = (
    reconciled
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.product_name"))
)

joined = (
    enriched_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.price").alias("product_price"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
)

totals = (
    joined
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("product_price")).otherwise(0)).alias("initech_product_price"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.avg(F.when(F.col("brand") == "Acme", F.col("product_price"))).alias("acme_avg_product_price"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count_2"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    totals
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "corr_quantity_unit_price",
        "stark_count_2",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("stark_count_2").asc_nulls_last())
)
