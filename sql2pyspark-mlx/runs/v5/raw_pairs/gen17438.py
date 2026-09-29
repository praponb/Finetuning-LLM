from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & ~F.col("city").isin(["London", "Chiang Mai", "New York"]))
    .select(
        "id",
        "country",
        "city",
        F.concat("city", F.lit(" - "), "supplier_name").alias("city_supplier_name"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "eco")), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Chiang Mai", "New York"]))
    .withColumn("country_upper", F.upper("country"))
    .withColumn("country_upper_2", F.upper("country"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

eligible_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= "2025-10-01"), "left_semi")
)

result = (
    eligible_suppliers
    .groupBy(
        F.when(F.col("price") >= 1000, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .filter(F.col("avg_price") > 0)
    .orderBy("avg_price")
)
