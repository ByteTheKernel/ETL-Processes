SELECT 
    product_type,
    decision_status,
    COUNT(*) AS cnt
FROM `etl-exam-source`.`processed/loan_applications/event_year=2026/`
WITH (FORMAT="parquet", SCHEMA=(
    product_type Utf8,
    decision_status Utf8
))
GROUP BY product_type, decision_status
ORDER BY product_type