from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "supplier_id").orderBy(F.col("tags_count").desc())
w2 = Window.partitionBy("country", "supplier_id").orderBy("tags_count", "id")
w3 = Window.partitionBy("country").orderBy("tags_count", "id")

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-03-30").cast("date")) & (~F.col("brand").eqNullSafe(F.lit("Umbrella"))))
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "tags",
        "supplier_id",
        F.size("tags").alias("tags_count"),
        F.trim("category").alias("clean_category"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.tags",
        "a.supplier_id",
        "a.tags_count",
        "a.resolved_brand",
        F.col("p.country"),
        F.col("p.supplier_name"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_products = (
    ranked
    .withColumn("prev_tags_count", F.lag("tags_count").over(w2))
    .withColumn("next_tags_count", F.lead("tags_count").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("next_tags_count_2", F.lead("tags_count", 2).over(w3))
)

result = (
    enriched_products
    .select(
        "id",
        "tags",
        "supplier_id",
        "tags_count",
        "resolved_brand",
        "country",
        "supplier_name",
        "prev_tags_count",
        "next_tags_count",
        F.when(F.col("supplier_name").rlike("^(Lion City Imports|Global Supply)$"), "OTHER_ZONE").when(F.col("supplier_name").rlike("^(Siam Parts Co|Tokyo Traders)$"), "EXTENDED").otherwise("CORE").alias("supplier_name_zone"),
    )
    .orderBy(F.col("supplier_name_zone").desc())
)
