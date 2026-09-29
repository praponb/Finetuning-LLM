from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("New York"))))
    .select(
        "id",
        "country",
        F.coalesce("supplier_name", F.lit("unassigned")).alias("supplier_name_filled"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "sale")), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name_filled",
        F.col("c.launch_date"),
        F.col("c.cost").alias("product_cost"),
    )
)

kept_keys = (
    with_latest
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("Sia%"))
        .select("id")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("product_cost") <= 390)
    .withColumn("launch_date_minus_52d", F.date_sub("launch_date", 52))
)

kept_suppliers = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") < F.col("s.launch_date_minus_52d")), "left_anti")
)

result = (
    kept_suppliers
    .rollup(F.date_trunc("week", "launch_date_minus_52d").alias("week_start"))
    .agg(
        F.max("product_cost").alias("max_product_cost"),
        F.percentile_approx("product_cost", 0.5).alias("product_cost_p50"),
        F.count("*").alias("cnt"),
        F.avg("product_cost").alias("avg_product_cost"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt_2"),
    )
    .filter(F.col("max_product_cost") < 1000000)
)
