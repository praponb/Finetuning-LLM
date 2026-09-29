from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.lower("supplier_name").alias("supplier_name_lower"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") <= 112), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "premium")), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.last("rating", ignorenulls=True).over(w2).alias("last_known_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

result = (
    latest_match
    .select(
        "id",
        "country",
        "rating",
        "supplier_name_lower",
        "city_filled",
        "last_known_rating",
        "launch_date",
        F.when(F.col("country").rlike("^(TH|UK)$"), "CORE").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("last_known_rating").desc())
)
