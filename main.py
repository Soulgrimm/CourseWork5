from src.utils import get_vacancies_company_hh, create_database, save_data_to_database
from config import config
from src.class_requests_db import DBManager


def main():
    # Получаем данные о компаниях и их вакансиях по API, создаем базу данных и таблицы, и заполняем их.
    params = config()
    data = get_vacancies_company_hh()
    create_database('datavacansies', params)
    save_data_to_database(data, 'datavacansies', params)
    testdb = DBManager('datavacansies', params)
    # Основной цикл программы работы с пользователем.
    while True:
        user_choice = int(input('Добрый день! Выберите действие:\n'
                                '1 - Получить список всех компаний и их вакансий.\n'
                                '2 - Получить список вакансий с указанием компании, зарплаты и ссылки на вакансию.\n'
                                '3 - Получить среднюю зарплату по вакансиям в рублях.\n'
                                '4 - Получить список вакансий у которых зп выше средней.\n'
                                '5 - Получить список всех вакансий, по введеному слову.\n'
                                '6 - Выйти.\n'))
        if user_choice == 1:
            data = testdb.get_companies_and_vacancies_count()
            for inf in data:
                print(f'Компания: {inf[0]}, Кол-во вакансий: {inf[1]};')
        elif user_choice == 2:
            data = testdb.get_all_vacancies()
            for inf in data:
                print(f'Компания: {inf[0]}, Вакансия: {inf[1]}, ЗП: {inf[2]}, Ссылка: {inf[3]};')
        elif user_choice == 3:
            print(testdb.get_avg_salary()[0][0])
        elif user_choice == 4:
            data = testdb.get_vacancies_with_higher_salary()
            for inf in data:
                print(f'Вакансия: {inf[0]}, ЗП: {inf[1]}')
        elif user_choice == 5:
            user_input = input('Введите слово для поиска: ')
            data = testdb.get_vacancies_with_keyword(user_input)
            for inf in data:
                print(f'Вакансия: {inf[0]}')
        elif user_choice == 6:
            testdb.conn_close()
            break


if __name__ == '__main__':
    main()
