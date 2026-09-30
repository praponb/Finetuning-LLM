from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("units_rounded", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-18").cast("date"))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.round("units", 0).alias("units_rounded"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "product_id",
        "revenue",
        "units_rounded",
        F.sum("units_rounded").over(w).alias("running_units_rounded"),
        F.first("revenue", ignorenulls=True).over(w2).alias("first_known_revenue"),
        F.lead("revenue", 1).over(w3).alias("next_revenue"),
    )
)

base = (
    scored
    .filter(F.col("running_units_rounded") < 110)
    .withColumn("greatest_first_known_revenue_units_rounded", F.greatest("first_known_revenue", "units_rounded"))
)

remaining_keys = (
    base
    .select("id", "product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units").between(30, 35))
        .select("id", "product_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("first_known_revenue") > 2000)
    .withColumn("adjusted_units_rounded", F.round((F.col("units_rounded") * 1.07), 2))
)

enriched = (
    valid
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.product_name").isNotNull())
    .select(
        "a.product_id",
        "a.revenue",
        "a.units_rounded",
        "a.running_units_rounded",
        "a.first_known_revenue",
        F.col("b.launch_date"),
        F.col("b.cost"),
        F.col("b.product_name"),
    )
)

result = (
    enriched
    .select("product_id", "cost", "revenue", "units_rounded")
    .unpivot(
        ids=["product_id"],
        values=["cost", "revenue", "units_rounded"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
