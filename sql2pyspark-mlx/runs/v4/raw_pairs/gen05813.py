from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("rating").between(2.5, 3.4))
    .withColumn("country_prefix", F.substring("country", 1, 2))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

with_latest = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

ranked = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date").between("2024-01-04", "2025-08-01")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "country",
        "city",
        "rating",
        "country_prefix",
        "city_filled",
        "cost",
        "rank_in_group",
    )
    .orderBy("rating", "cost")
)
