from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "brand").orderBy(F.col("last_restock").asc_nulls_last(), "id")

joined = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.cost"), F.col("p.price"), F.col("p.brand"))
)

metrics = (
    joined
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.sum("price").over(w2).alias("running_price"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("seq_no") >= 7, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("running_price")).otherwise(0)).alias("globex_running_price"),
    )
    .orderBy(F.col("globex_running_price").asc_nulls_last())
)
