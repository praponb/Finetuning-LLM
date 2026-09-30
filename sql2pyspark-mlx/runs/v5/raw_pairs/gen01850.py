from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.substring("supplier_name", 1, 2).alias("supplier_name_prefix"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_semi")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") >= 253), "left_semi")
)

with_last = (
    not_flagged_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

deduped = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "supplier_name_prefix",
        "resolved_city",
        "cost",
    )
)

filtered_suppliers = (
    deduped
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "*",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.length("supplier_name").alias("supplier_name_length"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

result = (
    filtered_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "supplier_name_prefix",
        "resolved_city",
        "is_new_york",
        "supplier_name_length",
        "city_filled",
        F.when(F.col("country").rlike("^(JP|TH)$"), "EXTENDED").when(F.col("country").rlike("^(US|SG)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("is_new_york").desc())
)
