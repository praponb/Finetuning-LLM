from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())
w3 = Window.orderBy(F.col("quantity").desc(), "id")
w4 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("row_seq", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("product_id").orderBy("quantity_rank", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("product_id").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("product_id").orderBy("quantity_rank", "id").rowsBetween(-6, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("units").alias("max_store_sale_units"))

ranked = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "product_id", "quantity")
)

metrics = (
    ranked
    .select(
        "id",
        "product_id",
        F.rank().over(w2).alias("quantity_rank"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

scored = (
    metrics
    .filter(F.col("quantity_rank") < 5)
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w4))
    .withColumn("quantity_rank_avg_2_prior", F.avg("quantity_rank").over(w5))
    .withColumn("row_seq_avg_6_prior", F.avg("row_seq").over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("row_seq_avg_6", F.avg("row_seq").over(w8))
)

remaining_keys = (
    scored
    .select("id")
    .exceptAll(
        df_inventory
        .filter(F.year("last_restock") >= 2024)
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.category"), F.col("b.cost"), F.col("b.product_name"))
)

joined_inventory = (
    joined
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select(
        "s.product_id",
        "s.row_seq_avg_6_prior",
        "s.row_seq_avg_6",
        "s.product_name",
        F.col("b.category").alias("product_category"),
        F.col("b.launch_date"),
    )
)

result = (
    joined_inventory
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "row_seq_avg_6_prior",
        "product_category",
        "launch_date",
        F.col("max_store_sale_units"),
    )
    .orderBy("product_category")
)
