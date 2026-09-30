from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= "2024-03-01"), "left_semi")
)

eligible_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") < 930), "left_anti")
)

valid = (
    eligible_suppliers
    .filter(F.col("country") == "JP")
    .select(
        "*",
        F.when(F.col("rating") >= 4.0, "High").when(F.col("rating") >= 2.3, "Medium").otherwise("Low").alias("rating_level"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.rating",
        F.col("c.cost").alias("product_cost"),
    )
)

result = (
    with_last
    .select(
        "id",
        "supplier_name",
        "product_cost",
        F.when(F.col("supplier_name").rlike("^(Pacific Wholesale|Global Supply)$"), "GROUP_B").when(F.col("supplier_name").rlike("^(Siam Parts Co|Tokyo Traders)$"), "CORE").otherwise("PRIORITY").alias("supplier_name_zone"),
    )
    .orderBy(F.col("product_cost").desc_nulls_last())
)
