from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w_recent = Window.partitionBy("city").orderBy("days_since_last_restock", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-02-18").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

filtered = (
    valid
    .filter((F.col("last_restock") >= F.lit("2023-07-18").cast("date")) & F.col("store_id").isin([2, 5]))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.days_since_last_restock",
        "a.last_restock_label",
        "a.quantity_filled",
        F.col("p.opened_date"),
        F.col("p.city"),
        F.col("p.store_name"),
    )
)

enriched = (
    with_dims
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("days_since_last_restock_avg_2", F.avg("days_since_last_restock").over(w_recent))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "quantity_filled",
        "opened_date",
        "city",
        "row_seq",
        "days_since_last_restock_avg_2",
        F.when(F.col("city").rlike("^(Bangkok|Tokyo)$"), "EXTENDED").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_A").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy("row_seq")
)
