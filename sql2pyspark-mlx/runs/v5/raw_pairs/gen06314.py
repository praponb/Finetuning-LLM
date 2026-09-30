from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w_ordered = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("sqft", "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

scored = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        F.lead("units", 1, 0).over(w).alias("next_units"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("revenue").over(w_ordered).alias("running_revenue"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.sqft") >= 4976)
    .select("e.id", "e.store_id", "e.next_units", "e.running_revenue", F.col("p.city"))
)

enriched_store_sales = (
    enriched
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.running_revenue",
        "s.city",
        F.col("p.opened_date"),
        F.col("p.store_name"),
    )
)

enriched_store_sales_2 = (
    enriched_store_sales
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.city",
        "s.opened_date",
        F.col("p2.city").alias("store_city"),
        F.col("p2.region"),
    )
)

ranked = (
    enriched_store_sales_2
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "store_city")
)

joined = (
    ranked
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        F.col("p.sqft"),
        F.col("p.store_name"),
        F.col("p.city"),
    )
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "store_name")
)

result = (
    latest
    .join(quantity_by_store_id, on="store_id", how="left")
    .select("id", "store_id", F.col("min_inventory_quantity"))
)
