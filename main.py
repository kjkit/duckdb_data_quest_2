# DATA_QUEST_2
# Analiza danych bibliotecznych jako przyklad uzycia DuckDB
import duckdb

# Ad.1️. Załaduj dane do bazy – zaimportuj pliki CSV do tabel Books, Members, BorrowedBook
# import data from files to duckdb memory db
duckdb.sql("DROP TABLE IF EXISTS books;")
duckdb.sql("""CREATE TABLE books AS 
            SELECT * FROM READ_CSV('data\Books.csv', COLUMN_NAMES=['id', 'title', 'author', 'genre', 'published_year', 'price']);""")

duckdb.sql("DROP TABLE IF EXISTS borrowed_books;")
duckdb.sql("""CREATE TABLE borrowed_books AS 
            SELECT * FROM READ_CSV('data\BorrowedBooks.csv', COLUMN_NAMES=['id', 'member_id', 'book_id', 'borrowed_date', 'returned_date']);""")

duckdb.sql("DROP TABLE IF EXISTS members;")
duckdb.sql("""CREATE TABLE members AS 
            SELECT * FROM READ_CSV('data\Members.csv', COLUMN_NAMES=['id', 'first_name', 'last_name', 'joined_date', 'membership_category']);""")

# Ad.2. Oblicz liczbę dni, przez które każda książka była wypożyczona – uwzględnij książki niezwrócone.
duckdb.sql("""select bb.book_id, b.title, 
                SUM(date_diff('day', bb.borrowed_date, coalesce(bb.returned_date, today()))) as total_borrow_days
                from borrowed_books bb 
                join books b on b.id = bb.book_id
                group by bb.book_id, b.title""")


#Ad.3. Znajdź wszystkich klientów, którzy dołączyli w ostatnich trzech miesiącach i posortuj według daty dołączenia.
duckdb.sql("""select *
                from members m
                where m.joined_date >= date_add(today(), - INTERVAL 3 MONTH)
                order by joined_date""")

#Ad.4. Pogrupuj książki według gatunku i oblicz całkowitą cenę książek w każdym gatunku.
duckdb.sql("""select b.genre, round(sum(b.price), 2) as total_price
                from books b
                group by b.genre""")

# Ad.5️. Znajdź liczbę książek wypożyczonych przez klientów każdej kategorii członkostwa – sprawdź, kto jest najbardziej aktywny.
duckdb.sql("""select m.membership_category, count(bb.id) as count_borrow_category
                from members m
                left join borrowed_books bb on bb.member_id = m.id 
                group by m.membership_category
                order by count(bb.id) DESC""")

# Ad.6. Wylistuj wszystkie książki wraz z imieniem i nazwiskiem klienta, który je wypożyczył
duckdb.sql("""select b.id, b.title, b.author, b.genre, b.published_year, b.price, 
                STRING_AGG(CONCAT(m.first_name, ' ', m.last_name), ', ' ORDER BY m.last_name DESC) as borrowed_name
                from books b 
                left join borrowed_books bb on bb.book_id = b.id 
                left join members m on m.id = bb.member_id 
                group by b.id, b.title, b.author, b.genre, b.published_year, b.price""")
 
# Ad.7. Znajdź klientów, którzy nie zwrócili książek
duckdb.sql("""select CONCAT(m.first_name, ' ',m.last_name) as borrowed_name, b.title, bb.borrowed_date
                from borrowed_books bb
                join books b on bb.book_id = b.id 
                join members m on bb.member_id = m.id
                where bb.returned_date is null""")

# Ad.8. Użyj CTE, aby znaleźć całkowitą liczbę książek wypożyczonych przez każdego członka, 
# i przefiltruj klientów, którzy wypożyczyli mniej niż 3 książki.
duckdb.sql("""WITH borrowed_books_count AS (
                    select CONCAT(m.first_name, ' ',m.last_name) as borrowed_name,  count(bb.book_id) as count_borrowed_books
                    from members m
                    left join borrowed_books bb on bb.member_id = m.id
                    group by CONCAT(m.first_name, ' ',m.last_name)
                )
                select * from borrowed_books_count bbc 
                where bbc.count_borrowed_books < 3""")

#9. Dodaj kolumnę klasyfikującą książki według ceny:
#"Niska" (Price < 10)
#"Średnia" (10 ≤ Price < 30)
#"Wysoka" (Price ≥ 30)
duckdb.sql("""select b.*,
              CASE 
                    WHEN b.price < 10 THEN 'Niska'
                    WHEN b.price >= 10 AND b.price < 30 THEN 'Średnia'
                    WHEN b.price >= 30 THEN 'Wysoka'
                END as price_category
                from books b""")


#10. Utwórz raport klasyfikujący klientów:
#"Nowi" – jeśli dołączyli w ciągu ostatniego roku.
#"Stali"
duckdb.sql("""select m.*,
                CASE
                    WHEN m.joined_date >= date_add(today(), - INTERVAL 1 YEAR) THEN 'Nowi'
                    WHEN m.joined_date < date_add(today(), - INTERVAL 1 YEAR) THEN 'Stali'
                END as membership_category
                from members m""")

#11 Znajdź klientów, którzy regularnie wypożyczają książki – np. co najmniej raz na dwa miesiące.
# TO DO
duckdb.sql("""select * from books b""")
duckdb.sql("""select * from borrowed_books bb""")
duckdb.sql("""select * from members m""")

duckdb.sql("""select min(bb.borrowed_date) as min_borrow, max(bb.borrowed_date) as max_borrow
                from borrowed_books bb""")

duckdb.sql("""WITH RECURSIVE date_month AS (
                    select CAST('2018-01-01' as DATE) AS generated_month
                    UNION ALL
                    select date_add(generated_month, INTERVAL 1 MONTH)
                    from date_month
                    where generated_month < CAST('2025-01-01'as DATE)
                    ),
                members_cross as (
                    select distinct m.id, dm.generated_month
                    from members m
                    join (select bb.member_id, min(bb.borrowed_date) as min_borrow_date, max(bb.borrowed_date) as max_borrow_date
                            from borrowed_books bb
                            group by bb.member_id
                    ) bb on bb.member_id = m.id
                    cross join date_month dm
                ),
                bcm as (
                    select m.id, m.generated_month, coalesce(mc.borrow_count_month, 0) as borrow_count_month
                    from members_cross m
                    left join (
                        select m.id, CAST(strftime(bb.borrowed_date, '%Y-%m-01') as DATE) as borrowed_month, count(bb.book_id) as borrow_count_month
                        from members m
                        join borrowed_books bb on bb.member_id = m.id
                        left join date_month dm on CAST(strftime(bb.borrowed_date, '%Y-%m-01') as DATE) = dm.generated_month
                        group by m.id, strftime(bb.borrowed_date, '%Y-%m-01')
                    ) mc on mc.id = m.id and mc.borrowed_month = m.generated_month
                )
                select *
                from bcm
                where id = 184
                order by generated_month""")


#12 Wykryj książki, które zalegają najdłużej oraz posortuj według liczby dni od wypożyczenia.
duckdb.sql("""select b.title, bb.borrowed_date, date_diff('day', bb.borrowed_date, coalesce(bb.returned_date, today())) as borrow_days
                from borrowed_books bb
                join books b on b.id = bb.book_id
                where bb.returned_date is null
                order by borrow_days desc""")


#13. Do zad.5 znajdź imiona i nazwiska top 10 osób.
duckdb.sql("""select CONCAT(m.first_name, ' ', m.last_name) as member_name, count(bb.id) as count_borrow_category
                from members m
                left join borrowed_books bb on bb.member_id = m.id 
                group by CONCAT(m.first_name, ' ', m.last_name)
                order by count(bb.id) DESC
                LIMIT 10""")

#14. Zidentyfikuj klientów, którzy wypożyczali książki przez minimum 3 kolejne miesiace ostatniego roku. "
# TO DO


