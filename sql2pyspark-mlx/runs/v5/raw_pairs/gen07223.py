from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date_plus_months", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("launch_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("brand")

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.add_months("launch_date", 6).alias("launch_date_plus_months"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("launch_date") < "2025-07-10")
    .withColumn("stock_qty_avg_5", F.avg("stock_qty").over(w))
    .withColumn("cumulative_cost", F.sum("cost").over(w2))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w3))
    .withColumn("cost_avg_3", F.avg("cost").over(w4))
    .withColumn("brand_min_stock_qty", F.min("stock_qty").over(w5))
)

cleaned_products = (
    with_windows
    .filter((F.col("launch_date") >= F.lit("2023-05-04").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "cumulative_cost",
        "corr_cost_stock_qty",
        "brand_min_stock_qty",
        F.coalesce("stock_qty_avg_5", F.lit(0)).alias("stock_qty_avg_5_filled"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

enriched = (
    cleaned_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.brand",
        "e.cost",
        "e.stock_qty",
        "e.tags",
        "e.supplier_id",
        "e.cumulative_cost",
        "e.brand_min_stock_qty",
        "e.stock_qty_avg_5_filled",
        F.col("p.supplier_name"),
        F.col("p.rating"),
        F.col("p.country"),
    )
)

filtered = (
    enriched
    .filter(F.col("cumulative_cost").isNotNull())
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "brand_min_stock_qty",
        "supplier_name",
        "rating",
        "country",
        F.greatest("cumulative_cost", "brand_min_stock_qty").alias("greatest_cumulative_cost_brand_min_stock_qty"),
        F.round((F.col("stock_qty_avg_5_filled") / F.col("rating")), 2).alias("stock_qty_avg_5_filled_per_rating"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "brand",
        "cost",
        "tags",
        "brand_min_stock_qty",
        "greatest_cumulative_cost_brand_min_stock_qty",
        "stock_qty_avg_5_filled_per_rating",
        "brand_filled",
        F.when(F.col("brand").rlike("^(Initech|Umbrella)$"), "CORE").otherwise("GROUP_A").alias("brand_zone"),
    )
    .orderBy("brand_zone")
)
