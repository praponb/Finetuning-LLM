from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("stock_qty", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("city_filled", F.coalesce("city", F.lit("unassigned")))
    .withColumn("city_clean", F.coalesce("city", F.lit("OTHER")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.city_clean",
        F.col("c.launch_date"),
        F.col("c.stock_qty"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") < F.col("s.launch_date")), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w2))
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
)

kept = (
    metrics
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") <= 243), "left_anti")
)

kept_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") < F.col("s.launch_date")), "left_semi")
)

not_flagged_suppliers = (
    kept_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Home", "Sports"])), "left_anti")
)

result = (
    not_flagged_suppliers
    .select(
        "id",
        "supplier_name",
        "city_clean",
        "launch_date",
        "stock_qty",
        "prev_stock_qty",
        "event_seq",
        F.when(F.col("supplier_name").rlike("^(Tokyo Traders|Global Supply)$"), "GROUP_A").when(F.col("supplier_name").rlike("^(Siam Parts Co|EuroGoods GmbH)$"), "EXTENDED").otherwise("CORE").alias("supplier_name_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), "event_seq")
)
