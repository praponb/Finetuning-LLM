from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("revenue_share_minus_revenue_share_minus_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("revenue_share", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("revenue_share_minus_revenue", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("product_id").orderBy(F.col("revenue_share_minus_revenue_share_minus_revenue").desc_nulls_last(), "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

enriched = (
    df_store_sales
    .select(
        "*",
        F.count("revenue").over(w).alias("product_id_count_revenue"),
        F.sum("units").over(w2).alias("cumulative_units"),
        F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4).alias("revenue_share"),
        F.sum("units").over(w4).alias("running_units"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
    )
)

base = (
    enriched
    .filter(F.col("units") != 18)
    .select(
        "id",
        "product_id",
        "sale_date",
        "cumulative_units",
        "revenue_share",
        "corr_revenue_units",
        (F.col("revenue_share") - F.col("revenue")).alias("revenue_share_minus_revenue"),
    )
)

cleaned = (
    base
    .filter(F.col("product_id").isin([9, 10]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "cumulative_units",
        "revenue_share",
        "revenue_share_minus_revenue",
        (F.col("revenue_share") - F.col("revenue_share_minus_revenue")).alias("revenue_share_minus_revenue_share_minus_revenue"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.cost"))
)

scored = (
    joined
    .withColumn("cumulative_cost", F.sum("cost").over(w5))
    .withColumn("revenue_share_minus_revenue_avg_5", F.avg("revenue_share_minus_revenue").over(w6))
    .withColumn("revenue_share_avg_6_prior", F.avg("revenue_share").over(w7))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "cumulative_units",
        "revenue_share",
        "revenue_share_minus_revenue_share_minus_revenue",
        "cost",
        "revenue_share_minus_revenue_avg_5",
        "revenue_share_avg_6_prior",
        F.col("avg_order_item_unit_price"),
    )
)
