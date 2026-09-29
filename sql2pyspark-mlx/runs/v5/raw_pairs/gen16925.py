from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("city") == "London")
    .select(
        "id",
        "country_upper",
        "city_filled",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

kept = (
    with_last
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") >= F.col("s.launch_date")), "left_anti")
)

valid = (
    kept
    .filter(F.col("launch_date") < "2025-04-03")
    .select(
        "id",
        "country_upper",
        "city_filled",
        "launch_date",
        F.date_trunc("MONTH", "launch_date").alias("launch_date_period"),
    )
)

totals = (
    valid
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min("launch_date_period").alias("started_at"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("num_rows") >= 1)
)

result = (
    totals
    .select("week_start", "year_start", "started_at")
    .orderBy("year_start")
)
