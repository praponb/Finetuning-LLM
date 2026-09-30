from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("store_sqft", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("revenue_int")
w_running = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-12").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.city"), F.col("p.region"), F.col("p.sqft"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.opened_date"), F.col("p.sqft").alias("store_sqft"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("revenue").over(w).alias("revenue_avg_6"),
        F.rank().over(w_grp).alias("revenue_int_rank"),
        F.sum("revenue").over(w_running).alias("running_revenue"),
    )
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "region",
        "revenue_int_rank",
        "running_revenue",
        F.when(F.col("region").rlike("^(South|East)$"), "GROUP_B").when(F.col("region").rlike("^(North|West)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("region_zone"),
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("units").desc(), "id")
)
