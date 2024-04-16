/*
 Завдання на SQL до лекції 03.
 */

/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT
    c.name as category
    , COUNT(fc.film_id) as films
FROM film_category fc
JOIN category c on fc.category_id = c.category_id
GROUP BY 1
ORDER BY 2 DESC;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT
    first_name || ' ' || last_name as actor_name
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film_actor fa ON fa.film_id = i.film_id
JOIN actor a ON a.actor_id = fa.actor_id
GROUP BY 1
ORDER BY COUNT(r.rental_id) DESC
LIMIT 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT
    c.name as category
FROM public.film f
JOIN public.film_category fc ON f.film_id = fc.film_id
JOIN public.category c ON fc.category_id = c.category_id
GROUP BY 1
ORDER BY SUM(f.rental_duration * f.rental_rate) DESC
LIMIT 1;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT
    f.title as film_title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE 1=1
    and i.film_id is NULL;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT
    first_name || ' ' || last_name as actor_name
FROM film_actor fa
JOIN film_category fc ON fa.film_id= fc.film_id
JOIN category c ON c.category_id = fc.category_id
JOIN actor a ON fa.actor_id = a.actor_id
WHERE 1=1
    and c.name = 'Children'
GROUP BY 1
ORDER BY COUNT(fa.film_id) DESC
LIMIT 3;
