from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost_whole", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("price").alias("min_product_price"))

valid = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Globex"]))
    .select(
        "*",
        F.floor("cost").alias("cost_whole"),
        F.floor("cost").alias("cost_whole_2"),
    )
)

scored = (
    valid
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "cost_whole",
        F.avg("cost").over(w).alias("cost_avg_2"),
        F.sum("cost_whole").over(w_grp).alias("running_cost_whole"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.rating").isNotNull())
    .select("s.*", F.col("p2.country"))
)

remaining_keys = (
    enriched
    .select("category", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isNotNull())
        .select("category", "supplier_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.month(F.col("x.sale_date")).isin([5, 9, 12])), "left_semi")
)

result = (
    not_flagged
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "tags",
        "supplier_id",
        "cost_whole",
        "cost_avg_2",
        "running_cost_whole",
        F.when(F.col("product_name").rlike("^(Running Shoes|Desk Lamp)$"), "GROUP_A").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "PRIORITY").otherwise("EXTENDED").alias("product_name_zone"),
        F.col("min_product_price"),
    )
    .orderBy(F.col("cost_avg_2").desc())
)
