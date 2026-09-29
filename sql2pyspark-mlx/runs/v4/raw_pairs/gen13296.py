from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("store_id").orderBy("units", "id")
w4 = Window.partitionBy("product_id", "store_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("sale_date", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("store_id")
w7 = Window.partitionBy("region").orderBy("corr_revenue_units", "id")
w8 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")

enriched = (
    df_store_sales
    .select(
        "*",
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
        F.lead("revenue", 2).over(w3).alias("next_revenue"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.sqft"), F.col("p.region"))
)

with_dims_store_sales = (
    with_dims
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.price"), F.col("b.category"))
)

enriched_store_sales = (
    with_dims_store_sales
    .select(
        "*",
        F.last("next_revenue", ignorenulls=True).over(w4).alias("last_known_next_revenue"),
        F.avg("corr_units_revenue").over(w5).alias("corr_units_revenue_avg_5_prior"),
        F.corr("revenue", "units").over(w6).alias("corr_revenue_units_2"),
        F.lead("corr_revenue_units", 1).over(w7).alias("next_corr_revenue_units"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

with_dims_store_sales_2 = (
    enriched_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.city"))
)

result = (
    with_dims_store_sales_2
    .groupBy(
        F.when(F.col("sqft") >= 16953, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        F.when(F.col("next_corr_revenue_units") >= 0.45, "PREMIUM").otherwise("STANDARD").alias("next_corr_revenue_units_tier"),
        F.when(F.col("last_known_next_revenue") >= 4000, "HIGH").otherwise("LOW").alias("last_known_next_revenue_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
)
