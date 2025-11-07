import asyncio
import logging
import pandas as pd
import time
import xml.etree.ElementTree as ET
from datetime import datetime
import os  # Для роботи з файлами

from core.rozetka_api import get_valid_token, build_items_cache
from core.commissions import load_commissions
from parsers.gamepro_parsers import parse_gamepro
from core.calculations import match_and_compare, calculate_new_prices

# Налаштування шляхів
OUTPUT_XML = "output/rozetka_optimized.xml"  # ФІКС: Додаємо папку output/
OLD_XML = "output/rozetka_optimized_old.xml"  # Для збереження старого XML


def parse_xml_to_dict(xml_file):
    """Парсить XML у dict {offer_id: {price, oldprice, available, stock_quantity, name}}."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        offers = root.findall(".//offer")
        xml_dict = {}
        for offer in offers:
            offer_id = offer.get("id")
            xml_dict[offer_id] = {
                "price": float(offer.findtext("price") or 0),
                "oldprice": float(offer.findtext("oldprice") or 0),
                "available": offer.get("available", "false"),
                "stock_quantity": int(offer.findtext("stock_quantity") or 0),
                "name": offer.findtext("name", "Невідома товар")
            }
        return xml_dict
    except Exception as e:
        logging.warning(f"Не вдалося парсити {xml_file}: {e}")
        return {}


def compare_xml_changes(old_xml, new_xml):
    """Порівнює старий і новий XML, повертає список змін."""
    old_dict = parse_xml_to_dict(old_xml)
    new_dict = parse_xml_to_dict(new_xml)
    changes = []

    for offer_id in set(old_dict.keys()) & set(new_dict.keys()):
        old = old_dict[offer_id]
        new = new_dict[offer_id]
        item_changes = []

        if old["price"] != new["price"]:
            item_changes.append(f"price: {old['price']} → {new['price']}")
        if old["oldprice"] != new["oldprice"]:
            item_changes.append(f"oldprice: {old['oldprice']} → {new['oldprice']}")
        if old["available"] != new["available"]:
            item_changes.append(f"available: {old['available']} → {new['available']}")
        if old["stock_quantity"] != new["stock_quantity"]:
            item_changes.append(f"stock_quantity: {old['stock_quantity']} → {new['stock_quantity']}")

        if item_changes:
            changes.append({
                "offer_id": offer_id,
                "name": old["name"][:50] + "..." if len(old["name"]) > 50 else old["name"],
                "changes": item_changes
            })

    logging.info(f"Знайдено {len(changes)} змінених товарів у XML")
    return changes


def generate_rozetka_xml(matches, output_file=OUTPUT_XML):
    """Генерує XML/YML для Rozetka з рекомендаціями."""
    # Копіюємо старий XML, якщо існує
    if os.path.exists(output_file):
        os.replace(output_file, OLD_XML)
        logging.info(f"Старий XML збережено як {OLD_XML}")

    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "My Shop"
    ET.SubElement(shop, "company").text = "My Company"
    ET.SubElement(shop, "url").text = "https://myshop.ua"
    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="UAH", rate="1")
    categories = ET.SubElement(shop, "categories")
    ET.SubElement(categories, "category", id="1").text = "Default"
    offers = ET.SubElement(shop, "offers")

    for match in matches:
        price_offer_id = match['price_offer_id']
        available = match['supplier']['available']
        final_price = match['recommendations']['final_price']
        old_price_rec = match['recommendations']['old_price_recommended']
        stock_qty = match['supplier']['stock_quantity']
        name = match['rozetka'].get('name', 'Невідома товар')

        offer = ET.SubElement(offers, "offer", id=price_offer_id, available=available)
        ET.SubElement(offer, "price").text = str(final_price)
        ET.SubElement(offer, "oldprice").text = str(old_price_rec)
        ET.SubElement(offer, "currencyId").text = "UAH"
        ET.SubElement(offer, "categoryId").text = "1"
        ET.SubElement(offer, "stock_quantity").text = str(stock_qty)
        ET.SubElement(offer, "name").text = name

    # ФІКС: Створюємо папку output, якщо не існує
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)  # '.' якщо dirname порожній
    tree_out = ET.ElementTree(yml)
    tree_out.write(output_file, encoding="utf-8", xml_declaration=True)
    logging.info(f"XML створено: {output_file} ({len(matches)} offers)")
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

    # РОЗРАХУНОК РЕКОМЕНДАЦІЙ ЦІН
    if total_matches > 0:
        print("\n=== РОЗРАХУНОК РЕКОМЕНДАЦІЙ ЦІН ===")
        start_time = time.perf_counter()
        updated_matches = calculate_new_prices(matches, df_comm)
        end_time = time.perf_counter()
        processing_time = end_time - start_time
        print(
            f"Час обробки {total_matches} матчів: {processing_time:.2f} секунд ({processing_time / total_matches:.4f} сек/товар)")

        # ГЕНЕРАЦІЯ EXCEL
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

        # НОВЕ: Порівняння старого і нового XML
        print("\n=== ПОРІВНЯННЯ XML ===")
        xml_start = time.perf_counter()
        generate_rozetka_xml(updated_matches)  # Створює новий XML
        if os.path.exists(OLD_XML):
            changes = compare_xml_changes(OLD_XML, OUTPUT_XML)
            if changes:
                print(f"\nЗмінено {len(changes)} товарів:")
                for i, change in enumerate(changes[:3]):
                    print(f"\n{i + 1}. Offer ID: {change['offer_id']}, Назва: {change['name']}")
                    print(f"   Зміни: {', '.join(change['changes'])}")
                    print("   ---")
                # Додаємо збереження змін у changes.xlsx
                changes_df = pd.DataFrame([
                    {
                        'Offer ID': change['offer_id'],
                        'Назва товару': change['name'],
                        'Зміни': '; '.join(change['changes'])
                    } for change in changes
                ])
                changes_df.to_excel('output/changes.xlsx', index=False, engine='openpyxl')
                print(f"Список змін збережено в output/changes.xlsx ({len(changes)} товарів)")
            else:
                print("\nЗмін у XML немає — все однакове!")
        else:
            print(f"\nСтарий XML ({OLD_XML}) відсутній — перший запуск")
        xml_end = time.perf_counter()
        print(f"\nЧас генерації XML і порівняння: {xml_end - xml_start:.2f} секунд")


if __name__ == "__main__":
    asyncio.run(main())