from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w_grp = Window.partitionBy("store_id")
w3 = Window.orderBy("last_restock_period", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("brand_length").desc())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("units").alias("avg_store_sale_units"))

base = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.quantity",
        "src.last_restock_period",
        "src.last_restock_label",
        "src.latest_rank",
        F.col("d.store_name"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.launch_date"), F.col("p.brand"), F.col("p.product_name"))
)

filtered = (
    enriched
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "last_restock_period",
        "last_restock_label",
        "latest_rank",
        "store_name",
        "launch_date",
        "brand",
        "product_name",
        F.date_trunc("YEAR", "last_restock_period").alias("last_restock_period_period"),
        F.length("brand").alias("brand_length"),
    )
)

metrics = (
    filtered
    .filter(F.col("brand_length") != 78)
    .select(
        "id",
        "store_id",
        "last_restock_label",
        "launch_date",
        "product_name",
        "last_restock_period_period",
        F.avg("brand_length").over(w_grp).alias("store_id_avg_brand_length"),
        F.row_number().over(w3).alias("row_seq"),
        F.dense_rank().over(w4).alias("brand_length_dense_rank"),
    )
)

daily = (
    metrics
    .groupBy(
        "store_id",
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("last_restock_period_period")) - F.unix_timestamp(F.min("last_restock_period_period"))) / 60).alias("span_minutes"),
        F.min("last_restock_period_period").alias("first_last_restock_period_period"),
        F.avg("row_seq").alias("avg_row_seq"),
    )
)

result = (
    daily
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "row_seq_tier",
        "span_minutes",
        "avg_row_seq",
        F.col("avg_store_sale_units"),
    )
)
