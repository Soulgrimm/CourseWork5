import psycopg2


class DBManager:
    def __init__(self, dbname, params):
        self.conn = psycopg2.connect(dbname=dbname, **params)

    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании."""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT company.company_name, COUNT(company_vacancies.name_vacansy) 
            FROM company 
            JOIN company_vacancies USING(company_id)
            GROUP BY company.company_name
            """)
            sorting = cur.fetchall()
        return sorting

    def get_all_vacancies(self):
        """получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT company.company_name, name_vacansy, salary, vacansy_url
            FROM company_vacancies
            JOIN company USING(company_id)""")
            sorting = cur.fetchall()
        return sorting

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT CAST(AVG(salary) AS INTEGER) 
            FROM company_vacancies
            WHERE currency = 'RUR'""")
            sorting = cur.fetchall()
        return sorting

    def get_vacancies_with_higher_salary(self):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT name_vacansy, salary
            FROM company_vacancies
            WHERE salary > (SELECT AVG(salary) FROM company_vacancies WHERE currency = 'RUR') AND currency = 'RUR'""")
            sorting = cur.fetchall()
        return sorting

    def get_vacancies_with_keyword(self, keyword):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT name_vacansy
            FROM company_vacancies
            WHERE name_vacansy LIKE '%{keyword}%'""")
            sorting = cur.fetchall()
        return sorting

    def conn_close(self):
        """Закрываем коннект."""
        self.conn.close()

    # def __del__(self):
    #     print('object delete')
