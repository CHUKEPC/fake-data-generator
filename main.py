import pandas as pd
from faker import Faker
import os
import time
import argparse
import sys

# Инициализация Faker
fake = Faker('ru_RU')


def generate_data_pandas(num_records=100):
    """
    Генерирует данные с помощью списка словарей и конвертирует в DataFrame.

    Args:
        num_records (int): Количество записей для генерации

    Returns:
        pd.DataFrame: DataFrame с сгенерированными данными
    """
    start_time = time.time()

    if num_records < 1:
        raise ValueError("Количество записей должно быть больше 0")

    if num_records > 900000:
        print("⚠️  Внимание: при генерации более 900,000 записей могут возникнуть конфликты уникальных ID")

    data = []
    try:
        for _ in range(num_records):
            data.append({
                "ID": fake.unique.random_int(min=100000, max=999999),
                "Имя": fake.name(),
                "Компания": fake.company(),
                "Должность": fake.job(),
                "Email": fake.email(),
                "IP-адрес": fake.ipv4(),
                "Дата регистрации": fake.date_this_decade().strftime("%Y-%m-%d"),
                "Описание": fake.text(max_nb_chars=100).replace('\n', ' ')
            })
    except Exception as e:
        raise RuntimeError(f"Ошибка при генерации данных: {e}")

    df = pd.DataFrame(data)

    elapsed_time = time.time() - start_time
    print(f"✓ Подготовка данных завершена за: {elapsed_time:.2f} сек.")
    return df


def save_all_formats(df, base_name="output/test_data"):
    """
    Сохраняет DataFrame в форматах CSV, JSON и TXT.

    Args:
        df (pd.DataFrame): DataFrame для сохранения
        base_name (str): Базовое имя файла (без расширения)

    Raises:
        OSError: При ошибках создания директории или записи файлов
    """
    try:
        # Создаем папку, если ее нет
        output_dir = os.path.dirname(base_name)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # Сохранение в CSV
        csv_path = f"{base_name}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')  # utf-8-sig для Excel
        print(f"✓ CSV сохранен: {csv_path}")

        # Сохранение в JSON
        json_path = f"{base_name}.json"
        df.to_json(json_path, orient='records', force_ascii=False, indent=4)
        print(f"✓ JSON сохранен: {json_path}")

        # Сохранение в TXT (используем табуляцию или фиксированный формат)
        txt_path = f"{base_name}.txt"
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(df.to_string(index=False))
        print(f"✓ TXT сохранен: {txt_path}")

        output_dir_display = output_dir if output_dir else "текущую директорию"
        print(f"\n✓ Все файлы успешно сохранены в {output_dir_display}/")

    except OSError as e:
        raise OSError(f"Ошибка при сохранении файлов: {e}")
    except Exception as e:
        raise RuntimeError(f"Неожиданная ошибка при сохранении: {e}")


def parse_arguments():
    """Парсит аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description='Генератор фейковых данных на русском языке',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py                    # Генерация 1000 записей (по умолчанию)
  python main.py -c 5000            # Генерация 5000 записей
  python main.py --count 10000 -o my_data  # Генерация 10000 записей с кастомным именем
        """
    )

    parser.add_argument(
        '-c', '--count',
        type=int,
        default=1000,
        help='Количество записей для генерации (по умолчанию: 1000)'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        default='output/test_data',
        help='Базовое имя файла без расширения (по умолчанию: output/test_data)'
    )

    return parser.parse_args()


def main():
    """Основная функция программы."""
    args = parse_arguments()

    # Валидация параметров
    if args.count < 1:
        print("❌ Ошибка: Количество записей должно быть больше 0", file=sys.stderr)
        sys.exit(1)

    if args.count > 1000000:
        print("⚠️  Внимание: Генерация более 1,000,000 записей может занять много времени и памяти")
        response = input("Продолжить? (y/n): ").lower()
        if response != 'y':
            print("Отменено пользователем")
            sys.exit(0)

    try:
        print(f"🚀 Запуск генерации {args.count:,} строк...")
        print("-" * 50)

        df_fake = generate_data_pandas(args.count)
        save_all_formats(df_fake, base_name=args.output)

        print("-" * 50)
        print(f"✅ Генерация завершена успешно! Создано {args.count:,} записей.")

    except ValueError as e:
        print(f"❌ Ошибка валидации: {e}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"❌ Ошибка файловой системы: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()