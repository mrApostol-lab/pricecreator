import pandas as pd
import logging


def load_commissions(excel_path='commissions.xlsx'):
    try:
        df = pd.read_excel(excel_path)
        df['Бренд'] = df['Бренд'].fillna('-')
        df['Діапазон цін'] = df['Діапазон цін'].fillna('-')
        logging.info(f"Завантажено {len(df)} рядків комісій")
        return df
    except Exception as e:
        logging.error(f"Помилка читання Excel: {e}")
        return pd.DataFrame()


def parse_range(range_str):
    if range_str == '-':
        return None, None
    try:
        min_max = range_str.split('-')
        return int(min_max[0].replace(' ', '')), int(min_max[1].replace(' ', ''))
    except:
        return None, None


def get_commission(df, hierarchy, brand, price):
    if df.empty:
        return 10.0

    brand_lower = str(brand).lower() if brand else '-'

    for level_name, level_id in reversed(hierarchy):
        logging.debug(f"Шукаємо по ID {level_id}, бренд {brand_lower}, ціна {price}")

        # По ID + бренд (якщо не '-')
        if brand_lower != '-':
            sub_df = df[(df['ID категорії'] == level_id) & (df['Бренд'].str.lower() == brand_lower)]
            if not sub_df.empty:
                comm = _get_from_sub_df(sub_df, price)
                if comm is not None:
                    return comm

        # Fallback по ID + бренд '-'
        sub_df = df[(df['ID категорії'] == level_id) & (df['Бренд'] == '-')]
        if not sub_df.empty:
            comm = _get_from_sub_df(sub_df, price)
            if comm is not None:
                logging.debug(f"Комісія з {level_name} (ID {level_id}): {comm}%")  # ЗМІНЕНО: debug замість info
                return comm

    logging.warning("Комісія не знайдена, дефолт 10%")
    return 10.0

def _get_from_sub_df(sub_df, price):
    base_row = sub_df[sub_df['Діапазон цін'] == '-']
    specific_rows = sub_df[sub_df['Діапазон цін'] != '-'].copy()

    if not specific_rows.empty:
        specific_rows['min_val'] = specific_rows['Діапазон цін'].apply(lambda x: parse_range(x)[0])
        specific_rows['max_val'] = specific_rows['Діапазон цін'].apply(lambda x: parse_range(x)[1])
        matching = specific_rows[(specific_rows['min_val'] <= price) & (specific_rows['max_val'] >= price)]
        if not matching.empty:
            return float(matching.iloc[0]['Відсоток комісії'])

    if not base_row.empty:
        return float(base_row.iloc[0]['Відсоток комісії'])

    return None