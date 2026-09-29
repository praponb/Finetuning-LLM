from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w_running = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("country", "city")
w4 = Window.partitionBy("city").orderBy("next_rating", "id")
w5 = Window.partitionBy("country", "city").orderBy("next_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("next_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy(F.col("event_seq").desc(), "id")
w8 = Window.partitionBy("country").orderBy(F.col("first_known_event_seq").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Initech", "Acme", "Globex"])), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.row_number().over(w_running).alias("event_seq"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

metrics_suppliers = (
    metrics
    .select(
        "*",
        F.lag("event_seq", 1, 0).over(w4).alias("prev_event_seq"),
        F.sum("event_seq").over(w5).alias("running_event_seq"),
        F.first("event_seq", ignorenulls=True).over(w6).alias("first_known_event_seq"),
        F.ntile(5).over(w7).alias("event_seq_bucket"),
    )
)

latest = (
    metrics_suppliers
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("city")
    .pivot("country", ["TH", "US", "SG"])
    .agg(F.avg("rating").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "city",
        F.col("TH_total").alias("th_total"),
        F.col("TH_cnt").alias("th_cnt"),
        F.col("US_total").alias("us_total"),
        F.col("US_cnt").alias("us_cnt"),
        F.col("SG_total").alias("sg_total"),
        F.col("SG_cnt").alias("sg_cnt"),
    )
)
