from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("launch_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc_nulls_first(), "id")
w3 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc_nulls_last(), "id")

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("tags_text", F.array_join("tags", "|"))
    .withColumn("tags_text_2", F.array_join("tags", ", "))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags_text",
    )
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_name", "brand", "launch_date")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

matched_keys = (
    ranked
    .select("id")
    .subtract(
        df_products
        .filter(F.col("category") == "Fashion")
        .select("id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id") == 27), "left_anti")
)

base = (
    eligible
    .filter((F.col("launch_date") >= F.lit("2023-05-30").cast("date")) & (F.col("launch_date") < "2024-09-06"))
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
)

result = (
    base
    .select(
        "product_name",
        "brand",
        "launch_date",
        F.when(F.col("brand").rlike("^(Initech|Acme)$"), "CORE").when(F.col("brand").rlike("^(Globex|Umbrella)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("brand_zone"),
    )
    .orderBy(F.col("launch_date").desc_nulls_first())
)
