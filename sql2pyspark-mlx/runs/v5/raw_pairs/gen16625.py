from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")

enriched = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    enriched
    .withColumn("year_start", F.date_trunc("year", "opened_date"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["seq_no", "year_start", "seq_no_tier"], ["seq_no", "year_start"], ["seq_no"], []],
        "seq_no",
        "year_start",
        "seq_no_tier",
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("region").alias("distinct_regions"),
        F.sum(F.when(F.col("city") == "New York", F.col("seq_no")).otherwise(0)).alias("new_york_seq_no"),
        F.countDistinct("region").alias("distinct_regions_2"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("seq_no").desc())
)
