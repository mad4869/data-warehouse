{{ config(schema='final') }}

WITH stg_book AS (
    SELECT
        book_id AS nk_book_id,
        title,
        isbn13,
        num_pages,
        publication_date,
        language_id,
        publisher_id
    FROM {{ ref("stg_dwh_book") }}
),

stg_author AS (
    SELECT *
    FROM {{ ref("stg_dwh_author") }}
),

stg_book_author AS (
    SELECT *
    FROM {{ ref("stg_dwh_book_author") }}
),

stg_book_language AS (
    SELECT *
    FROM {{ ref("stg_dwh_book_language") }}
),

stg_publisher AS (
    SELECT *
    FROM {{ ref("stg_dwh_publisher") }}
),

final_book AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["nk_book_id"]) }} AS sk_book_id,
        b.nk_book_id,
        b.title,
        b.isbn13,
        b.num_pages,
        b.publication_date,
        author_name AS author,
        language_name AS language,
        publisher_name AS publisher,
        {{ dbt_date.now() }} AS created_at,
        {{ dbt_date.now() }} AS updated_at
    FROM stg_book b
    INNER JOIN stg_book_author ba
        ON b.nk_book_id = ba.book_id
    INNER JOIN stg_author a
        ON ba.author_id = a.author_id
    INNER JOIN stg_book_language bl
        ON b.language_id = bl.language_id
    INNER JOIN stg_publisher p
        ON b.publisher_id = p.publisher_id
)

SELECT * FROM final_book