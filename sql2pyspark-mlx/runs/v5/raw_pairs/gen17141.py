from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Bangkok")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.ceil("rating").alias("rating_whole"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

remaining_keys = (
    base
    .select("country", "id")
    .intersect(
        df_suppliers
        .filter(F.length("supplier_name") < 8)
        .select("country", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand") != "Acme"), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 610), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty").between(148, 151)), "left_anti")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") >= "2023-08-01"), "left_anti")
)

result = not_flagged_suppliers.select("supplier_name", "country", "rating_whole")
