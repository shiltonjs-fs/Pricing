SELECT
    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
    sum(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) / sum(CARDUP_PAYMENT_USD_AMT) as TAKE_RATE
FROM
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE = 'business'
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-04-01')
    and CARDUP_PAYMENT_CUSTOMER_COMPANY_ID in (
        '2387',
        '590',
        '3109',
        '914',
        '768',
        '1844',
        '1649',
        '1431',
        '2235',
        '874',
        '4210',
        '805',
        '2677',
        '1182',
        '1443',
        '2967',
        '436',
        '405',
        '495',
        '1409'
    )
group by
    1;