from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("rating") < 3.9)
    .select(
        "id",
        "city",
        F.round("rating", 2).alias("rating_rounded"),
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.year(F.col("c.launch_date")) == 2025), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "city",
        "resolved_city",
        F.last("rating_rounded", ignorenulls=True).over(w).alias("last_known_rating_rounded"),
        F.max("rating_rounded").over(w2).alias("city_max_rating_rounded"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.cost").alias("product_cost"),
    )
)

result = (
    with_last
    .select(
        "id",
        "city",
        "product_cost",
        F.when(F.col("city").rlike("^(Singapore|Tokyo)$"), "CORE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy("city_zone", F.col("city").desc())
)
