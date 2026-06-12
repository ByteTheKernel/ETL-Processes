SELECT risk_level, decision_status, COUNT(*) AS cnt
FROM `etl-exam-source`.`processed/kafka_results/flat/part-00000-66367175-20e7-4d22-b2dd-c15003bd8c2c-c000.snappy.parquet`
WITH (FORMAT="parquet", SCHEMA=(
    application_id Utf8,
    customer_id Utf8,
    region Utf8,
    loan_amount Int64,
    term_months Int32,
    credit_score Int32,
    risk_level Utf8,
    decision_status Utf8,
    doc_type Utf8,
    doc_status Utf8,
    is_approved Int32,
    event_month Utf8,
    event_year Int32
))
GROUP BY risk_level, decision_status
ORDER BY risk_level, cnt DESC