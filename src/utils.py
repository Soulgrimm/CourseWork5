import json
from typing import Any
import psycopg2
import requests


def get_vacancies_company_hh() -> list[dict[str, Any]]:
    """Получаем по API информацию о компаниях и их вакансиях на HH."""
    hh_api_employers = 'https://api.hh.ru/employers'
    hh_api_vacancies = 'https://api.hh.ru/vacancies'

    list_company_name = ['2V Modules', 'АО Рут Код', 'amoCRM', 'ITENTIS GROUP', 'ООО СмартБрик', 'SberTech', 'KTS',
                         'ООО АВ Софт', 'KVINT', 'ООО Инспектор Клауд']
    data_company = []
    for company in list_company_name:
        params = {
            'text': f'{company}',
            'area': 1  # Москва
        }

        req = requests.get(hh_api_employers, params)
        data = req.content.decode()
        req.close()
        json_data_company = json.loads(data)['items'][0]

        params_vacancies = {
            'employer_id': f'{json_data_company["id"]}',
            'per_page': 5,
        }
        req = requests.get(hh_api_vacancies, params_vacancies)
        data_vacancies = req.content.decode()
        req.close()
        json_data_vacansy = json.loads(data_vacancies)['items']

        data_company.append({
            'company': json_data_company,
            'vacancies_company': json_data_vacansy
        })
    return data_company


def create_database(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных о компаниях и их вакансиях."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP DATABASE {database_name}")
        cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE company (
                company_id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                company_url TEXT,
                open_vacancies INTEGER
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE company_vacancies (
                vacansy_id SERIAL PRIMARY KEY,
                company_id INT REFERENCES company(company_id),
                name_vacansy VARCHAR(255) NOT NULL,
                vacansy_url TEXT,
                salary INTEGER,
                currency VARCHAR
            )
        """)

    conn.commit()
    conn.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
    """Сохранение данных о компаниях и их ваканиях в базу данных."""

    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        for company in data:
            company_data = company['company']
            cur.execute(
                """
                INSERT INTO company (company_name, company_url, open_vacancies)
                VALUES (%s, %s, %s)
                RETURNING company_id
                """,
                (company_data['name'], f"https://hh.ru/employer/{company_data['id']}",
                 company_data['open_vacancies']))

            company_id = cur.fetchone()[0]

            vacancies_data = company['vacancies_company']
            for vacansy in vacancies_data:
                if vacansy['salary'] is None:
                    continue
                elif vacansy['salary']['to'] is None:
                    avg_salary = vacansy['salary']['from']
                elif vacansy['salary']['from'] is None:
                    avg_salary = vacansy['salary']['to']
                else:
                    avg_salary = int((vacansy['salary']['to'] + vacansy['salary']['from']) / 2)

                cur.execute(
                    """
                    INSERT INTO company_vacancies (company_id, name_vacansy, vacansy_url, salary, currency)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (company_id, vacansy['name'], f"https://hh.ru/vacancy/{vacansy['id']}", avg_salary,
                     vacansy['salary']['currency']
                     ))
    conn.commit()
    conn.close()
