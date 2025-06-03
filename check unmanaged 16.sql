select
    *
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    true 
    and CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding', 'Scheduled')
    --AND CARDUP_PAYMENT_USER_TYPE = 'business'
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%collect%'
    and DATE(DATE_TRUNC('month', date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS))) >= DATE('2025-01-01')
    and DATE(DATE_TRUNC('month', date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS))) <= DATE('2025-03-01')
    and CARDUP_PAYMENT_CUSTOMER_COMPANY_ID in (
        '2778',
        '2779',
        '630',
        '2017',
        '1746',
        '3348',
        '2106',
        '2342',
        '3286',
        '793',
        '85',
        '1003',
        '4357',
        '1628',
        '3059',
        '2556',
        '1268',
        '4065',
        '3826'
    );