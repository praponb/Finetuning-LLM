from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id")
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("sale_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last(), "id")
w6 = Window.partitionBy("product_id", "category").orderBy("revenue", "id")
w7 = Window.partitionBy("category").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("first_known_revenue", "id")
w9 = Window.partitionBy("category")
w10 = Window.partitionBy("category", "product_id").orderBy("revenue", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2024-05-01").cast("date"))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.add_months("sale_date", 12).alias("sale_date_plus_months"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.lag("units", 1).over(w).alias("prev_units"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.sum("units").over(w3).alias("running_units"),
        F.first("revenue", ignorenulls=True).over(w4).alias("first_known_revenue"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.launch_date"), F.col("p.category"), F.col("p.price"))
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("prev_units", 1).over(w6).alias("prev_prev_units"),
        F.sum("units").over(w7).alias("running_units_2"),
        F.ntile(5).over(w8).alias("first_known_revenue_bucket"),
        F.round((F.col("revenue") / F.sum("revenue").over(w9)), 4).alias("revenue_share"),
    )
)

enriched_store_sales = (
    metrics
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.price").alias("product_price"))
)

latest = (
    enriched_store_sales
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select(
        "id",
        "category",
        F.col("corr_units_revenue").alias("CORR_UNITS_REVENUE_VAL"),
        F.col("first_known_revenue").alias("FIRST_KNOWN_REVENUE"),
        F.col("price").alias("PRICE"),
    )
    .unpivot(
        ["id", "category"],
        ["CORR_UNITS_REVENUE_VAL", "FIRST_KNOWN_REVENUE", "PRICE"],
        "measure",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
