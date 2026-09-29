from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")

joined = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.revenue",
        F.col("d.cost"),
    )
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    deduped
    .filter((F.col("sale_date") > F.lit("2023-08-23").cast("date")) & (F.col("revenue") > 2500))
    .select(
        "id",
        "store_id",
        "revenue",
        "cost",
        "rank_in_group",
        F.weekofyear("sale_date").alias("sale_date_weekofyear"),
    )
)

filtered = (
    valid
    .filter(F.col("cost") < 580)
    .select(
        "id",
        "revenue",
        "cost",
        "sale_date_weekofyear",
        (F.col("revenue") - F.col("sale_date_weekofyear")).alias("revenue_minus_sale_date_weekofyear"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("cost") >= 600, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("sale_date_weekofyear"), 2).alias("avg_sale_date_weekofyear"),
    )
)
