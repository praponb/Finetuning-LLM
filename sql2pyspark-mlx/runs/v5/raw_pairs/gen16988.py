from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "supplier_id").orderBy(F.col("cost").asc_nulls_last())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_first())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

matched_keys = (
    df_products
    .select("supplier_id", "brand")
    .subtract(
        df_products
        .filter(F.col("stock_qty").between(151, 305))
        .select("supplier_id", "brand")
    )
)

verified = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.rating") > 2.5)
    .select(
        "s.brand",
        "s.cost",
        "s.tags",
        "s.supplier_id",
        F.col("p2.supplier_name"),
        F.col("p2.country"),
        F.col("p2.city"),
    )
)

enriched_products = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w).alias("cost_percent_rank"),
        F.percent_rank().over(w2).alias("cost_percent_rank_2"),
    )
)

with_dims = (
    enriched_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.country").alias("supplier_country"),
        F.col("b.rating"),
        F.col("b.supplier_name").alias("supplier_supplier_name"),
    )
)

kept_keys = (
    with_dims
    .select("brand", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("category") == "Fashion")
        .select("brand", "supplier_id")
    )
)

verified_products = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

joined = (
    verified_products
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "inner")
    .select(
        "s.brand",
        "s.cost",
        "s.supplier_id",
        "s.supplier_name",
        "s.country",
        "s.city",
        "s.cost_percent_rank",
        "s.cost_percent_rank_2",
        "s.supplier_country",
        "s.rating",
        "s.supplier_supplier_name",
        F.col("p2.city").alias("supplier_city"),
    )
)

enriched_products_2 = (
    joined
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.rating").alias("supplier_rating"))
)

result = (
    enriched_products_2
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "brand",
        "supplier_id",
        "supplier_name",
        "city",
        "cost_percent_rank",
        "cost_percent_rank_2",
        "supplier_country",
        "supplier_city",
        "supplier_rating",
        F.when(F.col("supplier_name").rlike("^(Tokyo Traders|Global Supply)$"), "EXTENDED").when(F.col("supplier_name").rlike("^(Siam Parts Co|EuroGoods GmbH)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("supplier_name_zone"),
        F.col("max_product_price"),
    )
)
