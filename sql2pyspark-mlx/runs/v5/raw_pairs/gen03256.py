from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_running = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", F.col("c.cost"), F.col("c.price"))
)

enriched = (
    with_latest
    .filter(F.col("supplier_name").like("%Traders%"))
    .withColumn("running_count_price", F.count("price").over(w_running))
    .withColumn("running_sum_price", F.sum("price").over(w_running))
    .withColumn("running_max_price", F.max("price").over(w_running))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w3))
)

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("%Wholes%"))
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isin([27, 35])), "left_semi")
)

result = eligible.select("id", "cost", "price", "running_count_price")
