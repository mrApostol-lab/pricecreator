# Структура проекту: core/rozetka_api.py (виправлена версія)
# Виправлення: 1) Обробка битого кешу (try/except на json.load). 2) Кеш ієрархії по unique cat_id (не для кожного товару). 3) Додано warning для битого кешу. 4) Оптимізовано: спочатку collect unique_cat_ids, потім batch hierarchy.

import aiohttp
import asyncio
import json
import os
import base64
import logging
from datetime import datetime
from collections import defaultdict

# Налаштування
ROZETKA_AUTH_URL = "https://api-seller.rozetka.com.ua/sites"
ROZETKA_BASE_URL = "https://api-seller.rozetka.com.ua"
ROZETKA_USERNAME = "rgbtechhub"
ROZETKA_PASSWORD = "123qwe123"
TOKEN_FILE = "cache/token.json"
CACHE_FILE = "cache/rozetka_cache.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


async def login_to_rozetka():
    """Логін з base64-закодованим паролем, збереження токену."""
    encoded_password = base64.b64encode(ROZETKA_PASSWORD.encode("utf-8")).decode("utf-8")
    payload = {"username": ROZETKA_USERNAME, "password": encoded_password}
    async with aiohttp.ClientSession() as session:
        async with session.post(ROZETKA_AUTH_URL, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                token = data.get("content", {}).get("access_token")
                if token:
                    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
                    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
                        json.dump({"access_token": token}, f)
                    logging.info("Авторизація успішна, токен збережено")
                    return token
    logging.error("Не вдалося авторизуватися")
    return None


async def load_token():
    """Завантаження токену з файлу."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return data.get("access_token")
            except:
                return None
    return None


async def validate_token(token):
    """Валідація токену через тестовий запит."""
    url = f"{ROZETKA_BASE_URL}/goods/on-sale?page=1&pageSize=1"
    headers = {"Authorization": f"Bearer {token}", "Content-Language": "uk"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            return resp.status == 200


async def get_valid_token():
    """Головний етап: завантажити/валідація, якщо ні — логін."""
    token = await load_token()
    if token and await validate_token(token):
        logging.info("Токен валідний")
        print("Зайшли на Rozetka")
        return token
    logging.info("Токен відсутній або невалідний, авторизуємось заново")
    token = await login_to_rozetka()
    if token:
        print("Зайшли на Rozetka")
    else:
        print("Не вдалося зайти на Rozetka")
    return token


async def get_total_count(token):
    """Швидкий запит на загальну кількість товарів."""
    url = f"{ROZETKA_BASE_URL}/goods/on-sale"
    headers = {"Authorization": f"Bearer {token}", "Content-Language": "uk"}
    params = {"page": 1, "pageSize": 1}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                meta = data.get("content", {}).get("_meta", {})
                total = meta.get("totalCount", 0)
                logging.info(f"Загальна кількість активних товарів: {total}")
                return total
    logging.error("Не вдалося отримати totalCount")
    return 0


async def get_all_items(token, page_size=100):
    """Повний парсинг товарів з пагінацією."""
    total = await get_total_count(token)
    all_items = []
    page = 1
    while len(all_items) < total:
        url = f"{ROZETKA_BASE_URL}/goods/on-sale"
        headers = {"Authorization": f"Bearer {token}", "Content-Language": "uk"}
        params = {"page": page, "pageSize": page_size}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("content", {}).get("items", [])
                    if not items:
                        break
                    all_items.extend(items)
                    logging.info(f"Сторінка {page}: {len(items)} товарів (всього {len(all_items)}/{total})")
                    page += 1
                else:
                    logging.error(f"Помилка на сторінці {page}: {resp.status}")
                    break
    return all_items


async def get_category_by_id(token, cat_id):
    """Отримання однієї категорії за ID."""
    if not cat_id:
        return None
    url = f"{ROZETKA_BASE_URL}/market-categories/search"
    headers = {"Authorization": f"Bearer {token}", "Content-Language": "uk"}
    params = {"category_id": cat_id, "page": 1, "pageSize": 1}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                categories = data.get("content", {}).get("marketCategorys", [])
                if categories:
                    return categories[0]
    logging.warning(f"Не вдалося отримати категорію ID {cat_id}")
    return None


async def get_category_hierarchy(token, cat_id, max_depth=10):
    """Повна ієрархія від leaf до root."""
    if not cat_id:
        return []
    hierarchy = []
    current_id = cat_id
    visited = set()
    for _ in range(max_depth):
        if current_id in visited:
            logging.warning(f"Цикл в ієрархії від {cat_id}")
            break
        visited.add(current_id)
        cat = await get_category_by_id(token, current_id)
        if not cat:
            break
        hierarchy.append((cat.get("name"), current_id))
        parent_id = cat.get("parent_id")
        if not parent_id:
            break
        current_id = parent_id
    return list(reversed(hierarchy))  # Від root до leaf


def extract_brand(item):
    """Витяг бренду з товару."""
    brand = item.get('price_producer_name') or item.get('rz_producer', {}).get('name', 'Unknown')
    if brand == 'Unknown':
        import re
        name = item.get('name', '')
        match = re.match(r'^([A-Z][a-z]+(?: [A-Z][a-z]+)?) ', name)
        brand = match.group(1) if match else 'Unknown'
    return brand


async def build_items_cache(token, ttl_hours=24):
    """Парсинг + кеш: повертає dict товарів, зберігає кеш."""
    os.makedirs('cache', exist_ok=True)

    # Завантажити кеш (з обробкою помилок)
    cache = {}
    cache_valid = False
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            cache_time = datetime.fromisoformat(cache.get('timestamp', '2020-01-01T00:00:00'))
            if (datetime.now() - cache_time).total_seconds() / 3600 < ttl_hours:
                total_cached = cache.get('total_count', 0)
                current_total = await get_total_count(token)
                if total_cached == current_total:
                    cache_valid = True
                    logging.info(f"Кеш валідний: {total_cached} товарів")
                    print(f"Парсинг завершено з кешу: {total_cached} товарів")
                    return cache.get('items', {})
        except json.JSONDecodeError as e:
            logging.warning(f"Кеш биттий (JSON помилка): {e}. Робимо повний парсинг")
        except Exception as e:
            logging.warning(f"Помилка завантаження кешу: {e}. Робимо повний парсинг")

    if not cache_valid:
        # Повний парсинг
        items_raw = await get_all_items(token)
        items_dict = {}
        unique_cat_ids = set()

        # Спочатку collect unique cat_ids
        for item in items_raw:
            cat_id = item.get('price_category_id')
            if cat_id:
                unique_cat_ids.add(cat_id)

        # Batch: побудова hierarchy для unique cat_ids
        hierarchies = {}
        for cat_id in unique_cat_ids:
            hierarchy = await get_category_hierarchy(token, cat_id)
            hierarchies[cat_id] = hierarchy
            logging.info(f"Ієрархія для cat_id {cat_id}: {len(hierarchy)} рівнів")

        # Тепер для товарів
        for item in items_raw:
            rz_id = item.get('rz_item_id')
            if not rz_id:
                continue
            cat_id = item.get('price_category_id')

            # Базові дані
            item_data = {
                'name': item.get('name') or item.get('name_ua') or 'Невідома',
                'price': item.get('price', 0),
                'price_old': item.get('price_old', 0),
                'commission_percent': item.get('commission_percent', 0),
                'commission_sum': item.get('commission_sum', 0),
                'brand': extract_brand(item),
                'category_id': cat_id,
                'available': item.get('available', False),
                'stock_quantity': item.get('stock_quantity', 0),
                'price_offer_id': item.get('price_offer_id')
            }

            # Присвої hierarchy з кешу
            if cat_id and cat_id in hierarchies:
                item_data['hierarchy'] = [(name, id_) for name, id_ in hierarchies[cat_id]]
            else:
                item_data['hierarchy'] = []

            items_dict[rz_id] = item_data

        # Зберегти кеш
        cache_data = {
            'items': items_dict,
            'total_count': len(items_dict),
            'timestamp': datetime.now().isoformat()
        }
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        logging.info(f"Повний парсинг: {len(items_dict)} товарів, збережено кеш")
        print(f"Парсинг завершено: {len(items_dict)} товарів")
        return items_dict