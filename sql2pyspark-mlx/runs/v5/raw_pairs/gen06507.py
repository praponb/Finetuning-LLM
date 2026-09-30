from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("last_restock_dayofweek", "id")
w2 = Window.partitionBy("region").orderBy("last_restock_dayofweek", "id")
w3 = Window.orderBy(F.col("prev_quantity").desc(), "id")
w4 = Window.partitionBy("region", "store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w5 = Window.partitionBy("region").orderBy(F.col("last_restock").desc(), "id")
w6 = Window.partitionBy("region").orderBy("prev_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("region").orderBy("prev_quantity", "id")
w8 = Window.partitionBy("region").orderBy("quantity_str_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("region").orderBy("next_quantity", "id")
w_part = Window.partitionBy("region").orderBy("last_restock_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-06-02")))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") < "2023-10-01")
    .select(
        "e.id",
        "e.store_id",
        "e.quantity",
        "e.last_restock",
        "e.last_restock_dayofweek",
        "e.quantity_str",
        F.col("s.region"),
    )
)

metrics = (
    with_dims
    .select(
        "*",
        F.lead("quantity").over(w_grp).alias("next_quantity"),
        F.lag("quantity").over(w_grp).alias("prev_quantity"),
        F.lead("quantity_str").over(w_grp).alias("next_quantity_str"),
        F.lag("quantity_str").over(w_grp).alias("prev_quantity_str"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

scored = (
    metrics
    .withColumn("next_quantity_2", F.lead("quantity").over(w2))
    .withColumn("prev_prev_quantity_str", F.lag("prev_quantity_str").over(w2))
    .withColumn("next_prev_quantity_str", F.lead("prev_quantity_str").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("prev_quantity_bucket", F.ntile(10).over(w3))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter(F.col("region").isNotNull() & (F.col("next_quantity_2") >= 158))
    .select(
        "id",
        "last_restock",
        "last_restock_dayofweek",
        "region",
        "next_quantity",
        "prev_quantity",
        "seq_no",
        "next_quantity_2",
        "next_prev_quantity_str",
        "event_seq",
        "prev_quantity_bucket",
        F.ceil("quantity_str").alias("quantity_str_whole"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

deduped_inventory = (
    cleaned
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_inventory
    .select(
        "id",
        "last_restock_dayofweek",
        "region",
        "next_quantity",
        "prev_quantity",
        "seq_no",
        "next_quantity_2",
        "event_seq",
        "quantity_str_whole",
        "rn",
        F.last("last_restock_dayofweek", ignorenulls=True).over(w6).alias("last_known_last_restock_dayofweek"),
        F.ntile(4).over(w7).alias("prev_quantity_bucket_2"),
        F.last("seq_no", ignorenulls=True).over(w8).alias("last_known_seq_no"),
        F.lag("event_seq", 1).over(w9).alias("prev_event_seq"),
        F.sum("event_seq").over(w_part).alias("running_event_seq"),
    )
)
