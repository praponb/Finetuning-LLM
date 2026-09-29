from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("rating") <= 3.2)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("rating") >= 3.5, "High").when(F.col("rating") >= 2.0, "Medium").otherwise("Low").alias("rating_level"),
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"))
)

eligible = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") <= 400), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category").isin(["Sports", "Electronics", "Grocery"])), "left_anti")
)

eligible_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price").between(1060, 1150)), "left_anti")
)

result = (
    eligible_suppliers
    .rollup("category")
    .agg(
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.count("*").alias("cnt"),
        F.grouping("category").alias("gid"),
    )
)
