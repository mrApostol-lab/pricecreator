import asyncio
import logging
import pandas as pd  # Для таблиці рекомендацій
import time  # Для таймера
import xml.etree.ElementTree as ET  # Для XML
from datetime import datetime  # Для date в YML

from core.rozetka_api import get_valid_token, build_items_cache
from core.commissions import load_commissions
from parsers.gamepro_parsers import parse_gamepro  # GamePro з config
from core.calculations import match_and_compare, calculate_new_prices

# Налаштування FTP (не використовується, бо config — але лишаємо для сумісності)
OUTPUT_XML = "rozetka_optimized.xml"


def generate_rozetka_xml(matches, output_file=OUTPUT_XML):
    """Генерує XML/YML для Rozetka з рекомендаціями (на основі твого коду)."""
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")

    # Обов'язкові елементи Rozetka
    ET.SubElement(shop, "name").text = "My Shop"
    ET.SubElement(shop, "company").text = "My Company"
    ET.SubElement(shop, "url").text = "https://myshop.ua"

    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="UAH", rate="1")

    categories = ET.SubElement(shop, "categories")
    ET.SubElement(categories, "category", id="1").text = "Default"

    offers = ET.SubElement(shop, "offers")

    for match in matches:
        price_offer_id = match['price_offer_id']  # ФІКС: Використовуємо price_offer_id для матчу з постачальником
        available = match['supplier']['available']
        final_price = match['recommendations']['final_price']
        old_price_rec = match['recommendations']['old_price_recommended']
        stock_qty = match['supplier']['stock_quantity']
        name = match['rozetka'].get('name', 'Невідома товар')

        offer = ET.SubElement(offers, "offer", id=price_offer_id, available=available)  # ФІКС: ID = price_offer_id
        ET.SubElement(offer, "price").text = str(final_price)
        ET.SubElement(offer, "oldprice").text = str(old_price_rec)
        ET.SubElement(offer, "currencyId").text = "UAH"
        ET.SubElement(offer, "categoryId").text = "1"
        ET.SubElement(offer, "stock_quantity").text = str(stock_qty)
        ET.SubElement(offer, "name").text = name  # Додано назву для повноти

    # Збереження
    tree_out = ET.ElementTree(yml)
    tree_out.write(output_file, encoding="utf-8", xml_declaration=True)

    logging.info(f"XML для Rozetka створено: {output_file} ({len(matches)} offers)")
    print(f"XML готовий: {output_file} з {len(matches)} оновленими offers (завантаж у Rozetka).")


async def main():
    token = await get_valid_token()
    if not token:
        logging.error("Не можемо продовжити без токену")
        return

    # Завантаження Excel
    df_comm = load_commissions()

    # Парсинг Rozetka
    items_rozetka = await build_items_cache(token)

    # Парсинг GamePro з config
    items_supplier = parse_gamepro()

    # Співставлення
    matches, rozetka_only, supplier_only, differences, price_differences, same_price_count, same_old_price_count, same_available_count = match_and_compare(
        items_rozetka, items_supplier)

    # СТАТИСТИКА СПІВСТАВЛЕННЯ
    total_matches = len(matches)
    print(f"\n=== СТАТИСТИКА СПІВСТАВЛЕННЯ ({total_matches} матчів) ===")
    if total_matches > 0:
        print(f"Однакові ціни: {same_price_count} ({same_price_count / total_matches * 100:.1f}%)")
        print(f"Однакові старі ціни: {same_old_price_count} ({same_old_price_count / total_matches * 100:.1f}%)")
        print(f"Однакова наявність/stock: {same_available_count} ({same_available_count / total_matches * 100:.1f}%)")

    if price_differences:
        print(f"\nРізниці в цінах: {len(price_differences)} товарів")

    print(f"Тільки в Rozetka: {len(rozetka_only)}")
    print(f"Тільки в постачальнику: {len(supplier_only)}")

    # РОЗРАХУНОК РЕКОМЕНДАЦІЙ ЦІН З ТАЙМЕРОМ
    if total_matches > 0:
        print("\n=== РОЗРАХУНОК РЕКОМЕНДАЦІЙ ЦІН ===")
        start_time = time.perf_counter()

        updated_matches = calculate_new_prices(matches, df_comm)

        end_time = time.perf_counter()
        processing_time = end_time - start_time
        print(
            f"Час обробки {total_matches} матчів: {processing_time:.2f} секунд ({processing_time / total_matches:.4f} сек/товар)")

        # ГЕНЕРАЦІЯ EXCEL (як раніше)
        rec_list = []
        for match in updated_matches:
            rec = match['recommendations']
            name = match['rozetka'].get('name', 'Невідома')
            rec_list.append({
                'ID на Rozetka': match['id'],
                'Назва товару': name[:50] + '...' if len(name) > 50 else name,
                'Ціна на Rozetka зараз': match['rozetka']['price'],
                'Стара ціна на Rozetka зараз': match['rozetka']['old_price'],
                'Ціна закупки': match['supplier']['purchase_price'],
                'RRP у постачальника': match['supplier']['supplier_price'],
                'Стара ціна постачальника': match['supplier']['old_price'],
                'Фінальна ціна': rec['final_price'],
                'Мій базовий профіт': rec['my_profit_target'],
                'Мій реальний профіт': rec['net_profit'],
                'Відсоток Rozetka': rec['comm_used'],
                'Ітерації': rec['iterations_log']
            })

        df_rec = pd.DataFrame(rec_list)
        df_rec.to_excel('recommendations.xlsx', index=False, engine='openpyxl')
        print(f"\nРекомендації збережено в recommendations.xlsx ({len(rec_list)} товарів)")
        print("Вивід топ-10:")
        print(df_rec.head(10).to_string(index=False))

        # НОВЕ: Генерація XML для Rozetka
        print("\n=== ГЕНЕРАЦІЯ XML ДЛЯ ROZETKA ===")
        xml_start = time.perf_counter()
        generate_rozetka_xml(updated_matches)
        xml_end = time.perf_counter()
        print(f"Час генерації XML: {xml_end - xml_start:.2f} секунд")


if __name__ == "__main__":
    asyncio.run(main())