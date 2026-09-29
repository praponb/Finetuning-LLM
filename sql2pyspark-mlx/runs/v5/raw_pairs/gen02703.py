from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy("product_stock_qty_adjusted", "id")
w4 = Window.partitionBy("country").orderBy(F.col("product_cost").asc_nulls_last())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

cleaned = (
    with_latest
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round((F.col("product_stock_qty") * 1.15), 2).alias("product_stock_qty_adjusted"),
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") > "2023-08-01"), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("product_cost"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand").isin(["Initech", "Stark"])), "left_semi")
)

scored = (
    not_flagged
    .withColumn("prev_rating", F.lag("rating").over(w3))
    .withColumn("next_rating", F.lead("rating").over(w3))
    .withColumn("prev_product_cost", F.lag("product_cost").over(w3))
    .withColumn("next_product_cost", F.lead("product_cost").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

deduped = (
    scored
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "city",
        "product_stock_qty_adjusted",
        "country_lower",
        "product_cost",
        "prev_rating",
        "next_rating",
        "next_product_cost",
        "event_seq",
        F.when(F.col("city").rlike("^(Berlin|Tokyo)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
)
