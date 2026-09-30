from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("category", "supplier_id").orderBy("store_sale_units", "id")
w4 = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")

with_last = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.units").alias("store_sale_units"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.store_sale_revenue",
        "s.store_sale_units",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

enriched = (
    latest_match
    .select(
        "*",
        F.lead("store_sale_revenue").over(w_grp).alias("next_store_sale_revenue"),
        F.lag("store_sale_units").over(w_grp).alias("prev_store_sale_units"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.lead("revenue", 2).over(w4).alias("next_revenue"),
    )
)

base = (
    enriched
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        "store_sale_revenue",
        "store_sale_sale_date",
        "revenue",
        "prev_store_sale_units",
        "event_seq",
        "next_revenue",
        F.greatest("prev_store_sale_units", "event_seq").alias("greatest_prev_store_sale_units_event_seq"),
    )
)

result = (
    base
    .withColumn("year_start", F.date_trunc("year", "launch_date"))
    .rollup("year_start")
    .agg(
        F.percentile_approx("next_revenue", [0.25, 0.5, 0.75]).alias("next_revenue_quartiles"),
        F.percentile_approx("store_sale_revenue", 0.5).alias("store_sale_revenue_p50"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("greatest_prev_store_sale_units_event_seq")).otherwise(0)).alias("grocery_greatest_prev_store_sale_units_event_seq"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.grouping_id().alias("gid"),
    )
)
