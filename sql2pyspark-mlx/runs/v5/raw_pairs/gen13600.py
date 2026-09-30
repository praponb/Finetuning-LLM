from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-04-22").cast("date"))
    .select("*", F.round((F.col("stock_qty") * 0.85), 2).alias("stock_qty_adjusted"))
)

valid = (
    base
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.coalesce("stock_qty_adjusted", F.lit(0)).alias("stock_qty_adjusted_filled"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.country"), F.col("p.rating"), F.col("p.supplier_name"))
)

valid_products = (
    enriched
    .filter(~F.col("brand").isin(["Umbrella", "Acme"]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "stock_qty_adjusted",
        "stock_qty_adjusted_filled",
        "country",
        "supplier_name",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.when(F.col("cost") >= 720, "High").when(F.col("cost") >= 300, "Medium").otherwise("Low").alias("cost_tier"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

daily = (
    valid_products
    .groupBy("category")
    .agg(
        F.round(F.sum("stock_qty_adjusted_filled") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("stock_qty_adjusted_filled_per_acme"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.max(F.when(F.col("category") == "Home", F.col("cost"))).alias("home_max_cost"),
        F.percentile_approx("days_since_launch_date", [0.25, 0.5, 0.75]).alias("days_since_launch_date_quartiles"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.max("price").alias("max_price"),
    )
    .filter(F.col("max_price") < 1000000)
)

result = (
    daily
    .select(
        "category",
        "stock_qty_adjusted_filled_per_acme",
        "num_categories",
        "home_max_cost",
        "days_since_launch_date_quartiles",
        "cost_quartiles",
        "max_price",
    )
    .orderBy("max_price")
)
