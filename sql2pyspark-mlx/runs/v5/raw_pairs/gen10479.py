from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy("corr_quantity_unit_price", "id")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("product_id", "order_id")
w6 = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price_int").asc_nulls_first())
w7 = Window.partitionBy("product_id").orderBy("order_id_max_corr_quantity_unit_price")

enriched = (
    df_order_items
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id").eqNullSafe(F.col("b.id")), "left")
    .select("a.*", F.col("b.cost"))
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.avg("quantity").over(w2).alias("quantity_avg_5"),
    )
)

scored = (
    enriched_order_items
    .filter(F.col("product_id").isin([3, 36]))
    .select(
        "*",
        F.row_number().over(w_running).alias("row_seq"),
        F.max("corr_quantity_unit_price").over(w4).alias("order_id_max_corr_quantity_unit_price"),
    )
)

remaining_keys = (
    scored
    .select("order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id") == 9)
        .select("order_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched_order_items_2 = (
    verified
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.product_name"), F.col("d.stock_qty"))
)

cleaned = (
    enriched_order_items_2
    .filter(F.col("quantity").isNotNull())
    .select(
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "cost",
        "quantity_avg_5",
        "row_seq",
        "order_id_max_corr_quantity_unit_price",
        "product_name",
        "stock_qty",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

scored_order_items = (
    cleaned
    .select(
        "*",
        F.max("unit_price_int").over(w5).alias("product_id_max_unit_price_int"),
        F.round((F.col("quantity_avg_5") / F.sum("quantity_avg_5").over(w4)), 4).alias("quantity_avg_5_share"),
        F.cume_dist().over(w6).alias("unit_price_int_cume_dist"),
        F.stddev_samp("quantity_avg_5").over(w).alias("product_id_stddev_samp_quantity_avg_5"),
        F.dense_rank().over(w7).alias("order_id_max_corr_quantity_unit_price_dense_rank"),
    )
)

result = (
    scored_order_items
    .select("product_id", "unit_price_int_cume_dist", "cost", "unit_price")
    .unpivot(
        ids=["product_id"],
        values=["unit_price_int_cume_dist", "cost", "unit_price"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
