from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("days_since_sale_date").desc_nulls_first(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

latest = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

valid = (
    latest
    .filter(F.col("sale_date") >= F.lit("2023-08-20").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.round("revenue", 1).alias("revenue_rounded"),
    )
)

valid_store_sales = (
    valid
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-04-20")))
    .select(
        "*",
        F.round((F.col("revenue_rounded") / F.col("units")), 2).alias("revenue_rounded_per_units"),
    )
)

joined = (
    valid_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.price"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

matched_keys = (
    deduped
    .select("product_id", "id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([2, 4, 5, 6]))
        .select("product_id", "id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "days_since_sale_date",
        "revenue_rounded",
        "price",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("sale_date").desc())
)
