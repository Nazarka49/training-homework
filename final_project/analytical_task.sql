#В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду вересня?
SELECT
    upe.state
    , count(1) as sales
FROM de-07-nazar-meliukh.silver.sales s
JOIN de-07-nazar-meliukh.gold.user_profiles_enriched upe ON s.client_id = upe.client_id
WHERE 1=1
    and s.purchase_date BETWEEN DATE('2022-09-01') and DATE('2022-09-10')
    and s.product ='TV'
    and DATE_DIFF(CURRENT_DATE, upe.birth_date, YEAR) BETWEEN 20 and 30
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1

#Відповідь: Iowa
