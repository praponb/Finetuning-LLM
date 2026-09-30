from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating_whole").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")

base = (
    df_suppliers
    .filter(F.col("rating") < 2.4)
    .withColumn("supplier_name_filled", F.coalesce("supplier_name", F.lit("unassigned")))
    .withColumn("rating_whole", F.floor("rating"))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "country", "rating", "rating_whole")
)

kept = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= "2025-05-01"), "left_semi")
)

ranked = (
    kept
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "country",
        "rating",
        "rating_whole",
        "rn",
        F.when(F.col("country").rlike("^(UK|US)$"), "CORE").when(F.col("country").rlike("^(TH|SG)$"), "PRIORITY").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("country").desc())
)
