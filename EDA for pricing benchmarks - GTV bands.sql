SELECT
    case
                when CARDUP_PAYMENT_USD_AMT < 50000 then '01. 0-50k'
                when CARDUP_PAYMENT_USD_AMT < 100000 then '02. 50-100k'
                when CARDUP_PAYMENT_USD_AMT < 150000 then '03. 100-150k'
                when CARDUP_PAYMENT_USD_AMT < 200000 then '04. 150-200k'
                else '05. 200k+'
            end as USD_AMT_BAND,
            count(distinct dwh_cardup_payment_id) as NUM_TRANSACTIONS,
FROM
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE IN ('business')
    AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    AND DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-05-01')
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    AND CARDUP_PAYMENT_CARD_TYPE IN ('Visa', 'Mastercard')
group by 1