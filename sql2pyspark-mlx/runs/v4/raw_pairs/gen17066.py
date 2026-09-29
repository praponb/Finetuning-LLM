from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.orderBy(F.col("cost").desc())
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

kept_keys = (
    df_inventory
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 5)
        .select("product_id", "id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.opened_date"), F.col("d.store_name"))
)

enriched = (
    with_dims
    .withColumn("product_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w_running))
    .withColumn("store_id_max_cost", F.max("cost").over(w2))
    .withColumn("cost_rank", F.rank().over(w3))
)

result = (
    enriched
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "cost",
        "store_name",
        "product_id_stddev_samp_quantity",
        "cost_rank",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Old Town)$"), "OTHER_ZONE").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "EXTENDED").otherwise("CORE").alias("store_name_zone"),
        F.col("avg_store_sale_revenue"),
    )
)
