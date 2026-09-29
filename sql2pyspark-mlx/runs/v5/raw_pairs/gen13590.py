from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.supplier_name_slug",
        F.col("c.launch_date"),
        F.col("c.category"),
    )
)

enriched = (
    with_last
    .filter(F.month("launch_date").isin([1, 3, 9]))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w2))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"))
)

result = (
    with_latest
    .groupBy(F.date_trunc("quarter", "launch_date").alias("quarter_start"))
    .agg(
        F.count_if(F.col("product_category") == "Home").alias("home_events"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("event_count_2"),
        F.count_if(F.col("product_category") == "Grocery").alias("grocery_events"),
    )
    .filter(F.col("grocery_events") > 1)
    .orderBy(F.col("grocery_events").desc())
)
