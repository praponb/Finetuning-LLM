from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("cost_avg_3_tier").orderBy(F.col("distinct_ids").desc())

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("stock_qty_str", F.col("stock_qty").cast("string"))
)

enriched = (
    valid
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "inner")
    .select("a.id", "a.brand", "a.cost", "a.supplier_id", F.col("b.city"))
)

valid_products = (
    enriched
    .filter(F.col("city") != "Tokyo")
    .select(
        "id",
        "brand",
        "cost",
        "supplier_id",
        "city",
        F.substring("city", 1, 4).alias("city_prefix"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

with_windows = (
    valid_products
    .select(
        "id",
        "brand",
        "supplier_id",
        "city",
        F.avg("cost").over(w).alias("cost_avg_6"),
        F.avg("cost").over(w2).alias("cost_avg_3"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cost_avg_3") >= 686, "LARGE").otherwise("SMALL").alias("cost_avg_3_tier"),
        "brand",
        "supplier_id",
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost_avg_6")).otherwise(0)).alias("globex_cost_avg_6"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost_avg_3")).otherwise(0)).alias("globex_cost_avg_3"),
    )
    .withColumn("distinct_ids_dense_rank", F.dense_rank().over(w3))
)
