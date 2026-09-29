from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "city").orderBy("quantity", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("quantity", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w_recent = Window.partitionBy("product_id").orderBy(F.col("quantity_avg_5_prior").asc_nulls_last())
w_part = Window.partitionBy("city", "product_id").orderBy("cumulative_quantity", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w8 = Window.partitionBy("product_id", "city").orderBy("rn", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("store_id").orderBy("quantity_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy("quantity_avg_5_prior_avg_6_prior", "id")
w11 = Window.partitionBy("store_id").orderBy("quantity_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("city").orderBy("quantity_avg_5_prior", "id")
w13 = Window.partitionBy("store_id", "city").orderBy(F.col("quantity_avg_5_prior_percent_rank").desc_nulls_last())

with_dims = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.quantity",
        F.col("s.city"),
        F.col("s.store_name"),
    )
)

scored = (
    with_dims
    .withColumn("cumulative_quantity", F.sum("quantity").over(w_running))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w3))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "city",
        "store_name",
        "quantity_avg_6_prior",
        "quantity_avg_5_prior",
        "rn",
        F.percent_rank().over(w_recent).alias("quantity_avg_5_prior_percent_rank"),
        F.avg("quantity_avg_5_prior").over(w_part).alias("quantity_avg_5_prior_avg_3_prior"),
        F.ntile(4).over(w7).alias("quantity_bucket"),
        F.avg("quantity_avg_5_prior").over(w8).alias("quantity_avg_5_prior_avg_6_prior"),
        F.last("quantity_avg_5_prior", ignorenulls=True).over(w9).alias("last_known_quantity_avg_5_prior"),
    )
)

scored_inventory = (
    with_windows
    .filter(F.col("quantity_bucket") <= 7)
    .select(
        "id",
        "product_id",
        "city",
        "store_name",
        "quantity_avg_5_prior_percent_rank",
        "quantity_avg_5_prior_avg_3_prior",
        "quantity_bucket",
        "quantity_avg_5_prior_avg_6_prior",
        "last_known_quantity_avg_5_prior",
        F.row_number().over(w10).alias("event_seq"),
        F.last("last_known_quantity_avg_5_prior", ignorenulls=True).over(w11).alias("last_known_last_known_quantity_avg_5_prior"),
        F.row_number().over(w12).alias("row_seq"),
        F.dense_rank().over(w13).alias("quantity_avg_5_prior_percent_rank_dense_rank"),
    )
)

result = (
    scored_inventory
    .select(
        "id",
        "city",
        "store_name",
        "quantity_avg_5_prior_percent_rank",
        "quantity_avg_5_prior_avg_6_prior",
        "last_known_quantity_avg_5_prior",
        "event_seq",
        "last_known_last_known_quantity_avg_5_prior",
        "row_seq",
        F.when(F.col("city").rlike("^(Berlin|Tokyo)$"), "GROUP_A").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("last_known_quantity_avg_5_prior").desc_nulls_first())
)
