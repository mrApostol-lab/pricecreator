# core/calculations.py
import logging
import math


def match_and_compare(rozetka_items, supplier_dict):
    matches = []
    rozetka_only = []
    supplier_only = []
    same_price_count = 0
    same_old_price_count = 0
    same_available_count = 0
    differences = []
    price_differences = []

    for rz_id, rz_data in rozetka_items.items():
        price_offer_id = rz_data.get('price_offer_id')
        if not price_offer_id:
            continue

        if price_offer_id in supplier_dict:
            sup_data = supplier_dict[price_offer_id]
            match = {
                'id': rz_id,
                'price_offer_id': price_offer_id,
                'rozetka': {
                    'price': rz_data['price'],
                    'old_price': rz_data['price_old'],
                    'available': rz_data['available'],
                    'stock_quantity': rz_data['stock_quantity'],
                    'name': rz_data.get('name', 'Невідома'),  # ФІКС: Додано з кешу
                    'hierarchy': rz_data.get('hierarchy', []),  # ФІКС: Додано з кешу
                    'brand': rz_data.get('brand', '-')  # ФІКС: Додано з кешу
                },
                'supplier': {
                    'purchase_price': sup_data['purchase_price'],
                    'supplier_price': sup_data['supplier_price'],
                    'old_price': sup_data['old_price'],
                    'available': sup_data['available'],
                    'stock_quantity': sup_data['stock_quantity'],
                    'is_rrp_fallback': sup_data.get('is_rrp_fallback', False)
                },
                'match': True
            }
            matches.append(match)

            # Перевірка однаковості
            if match['rozetka']['price'] == match['supplier']['supplier_price']:
                same_price_count += 1
            else:
                delta = match['rozetka']['price'] - match['supplier']['supplier_price']
                match['price_delta'] = delta
                price_differences.append(match)

            if match['rozetka']['old_price'] == match['supplier']['old_price']:
                same_old_price_count += 1
            if match['rozetka']['available'] == match['supplier']['available'] and match['rozetka']['stock_quantity'] == \
                    match['supplier']['stock_quantity']:
                same_available_count += 1

            diffs = []
            if match['rozetka']['price'] != match['supplier']['supplier_price']:
                diffs.append(
                    f"price: {match['rozetka']['price']} vs {match['supplier']['supplier_price']} (delta {delta:+.0f})")
            if match['rozetka']['old_price'] != match['supplier']['old_price']:
                diffs.append(f"old_price: {match['rozetka']['old_price']} vs {match['supplier']['old_price']}")
            if match['rozetka']['available'] != match['supplier']['available'] or match['rozetka']['stock_quantity'] != \
                    match['supplier']['stock_quantity']:
                diffs.append(
                    f"available/stock: {match['rozetka']['available']}/{match['rozetka']['stock_quantity']} vs {match['supplier']['available']}/{match['supplier']['stock_quantity']}")

            if diffs:
                match['differences'] = diffs
                differences.append(match)
        else:
            rozetka_only.append((rz_id, price_offer_id))

    for sup_id in supplier_dict:
        if sup_id not in [data.get('price_offer_id') for data in rozetka_items.values() if data.get('price_offer_id')]:
            supplier_only.append(sup_id)

    logging.info(f"Співставлення по price_offer_id: {len(matches)} матчів")
    logging.info(
        f"Однакові ціни: {same_price_count}, старі ціни: {same_old_price_count}, наявність/stock: {same_available_count}")
    logging.info(f"Відмінності: {len(differences)} товарів, з них різниця в ціні: {len(price_differences)}")

    return matches, rozetka_only, supplier_only, differences, price_differences, same_price_count, same_old_price_count, same_available_count
def get_differences_summary(differences):
    if not differences:
        return "Немає відмінностей"
    summary = {}
    for diff in differences:
        for d in diff['differences']:
            if d not in summary:
                summary[d] = 1
            else:
                summary[d] += 1
    return summary


def get_profit_target(cost):
    """Визначає target my_profit на основі cost (purchase_price)."""
    if cost < 3000:
        return max(cost * 0.20, 100)
    elif 3000 <= cost <= 3999:
        return 500
    elif 4000 <= cost <= 5999:
        return 600
    elif 6000 <= cost <= 7999:
        return 700
    else:  # >= 8000
        return 1000


def round_price(price, cost, my_profit_target, df_comm, hierarchy, brand, supplier_price):
    """Округляє price до 9/49/99 залежно від типу, з перевіркою net >= target."""
    from core.commissions import get_commission

    # Визначення ending і кроку
    if price < 500:
        endings = [9]
        step = 10  # Для дешевих
    elif price <= 5000:
        endings = [99]
        step = 100
    else:
        endings = [99, 49]  # Пріоритет 99, fallback 49
        step = 100

    # Знаходимо найближче вгору (спрощенно: до наступного десятка/сотні з ending)
    if price < 500:
        # Для 9: math.ceil(price / 10) * 10 - 1
        rounded = math.ceil(price / 10) * 10 - 1
    else:
        # Для 99: math.ceil(price / 100) * 100 - 1
        rounded = math.ceil(price / 100) * 100 - 1

    # Ітерація для перевірки net
    while True:
        comm = get_commission(df_comm, hierarchy, brand, rounded)
        net = rounded - cost - (rounded * comm / 100)
        if net >= my_profit_target:
            break
        rounded += step  # Додаємо крок і повторюємо округлення

    return max(rounded, supplier_price)


def binary_search_price(cost, my_profit_target, df_comm, hierarchy, brand, supplier_price):
    """Бінарний пошук найменшої ціни з net >= target."""
    from core.commissions import get_commission

    low = int(cost + my_profit_target)  # Мінімум
    high = 100000  # Максимум
    iterations = []  # Лог

    while low < high:
        mid = (low + high) // 2
        comm = get_commission(df_comm, hierarchy, brand, mid)
        net = mid - cost - (mid * comm / 100)

        iterations.append(f"{low}-{high}, mid={mid} net={net:.0f}")

        if net >= my_profit_target:
            high = mid  # Можна нижче
        else:
            low = mid + 1  # Потрібно вище

    iterations.append(
        f"Фінал: {low} net={(low - cost - (low * get_commission(df_comm, hierarchy, brand, low) / 100)):.0f}")
    base_price = max(low, supplier_price)  # Анти-демпінг
    return base_price, '; '.join(iterations[:10]) + '...' if len(iterations) > 10 else '; '.join(
        iterations)  # Короткий лог


def calculate_new_prices(matches, df_comm):
    """Розраховує рекомендації: бінарний пошук + округлення + old_price логіка."""
    from core.commissions import get_commission

    for match in matches:
        rz_data = match['rozetka']
        sup_data = match['supplier']
        cost = sup_data['purchase_price']
        supplier_price = sup_data['supplier_price']
        supplier_old_price = sup_data['old_price']
        is_rrp_fallback = sup_data['is_rrp_fallback']
        rozetka_price = rz_data['price']
        hierarchy = rz_data.get('hierarchy', [])
        brand = rz_data.get('brand', '-')

        my_profit_target = get_profit_target(cost)

        # RRP чек
        comm_rrp = get_commission(df_comm, hierarchy, brand, supplier_price)
        rrp_net = supplier_price - cost - (supplier_price * comm_rrp / 100)
        used_rrp = False
        if rrp_net > my_profit_target:
            base_price = supplier_price
            net_profit = rrp_net
            comm_used = comm_rrp
            iterations_log = "Використано RRP (вигідніше)"
            used_rrp = True
        else:
            used_rrp = False
            # БІНАРНИЙ ПОШУК
            base_price, iterations_log = binary_search_price(cost, my_profit_target, df_comm, hierarchy, brand,
                                                             supplier_price)
            comm_used = get_commission(df_comm, hierarchy, brand, base_price)
            net_profit = base_price - cost - (base_price * comm_used / 100)

        # Округлення
        final_price = round_price(base_price, cost, my_profit_target, df_comm, hierarchy, brand, supplier_price)
        comm_final = get_commission(df_comm, hierarchy, brand, final_price)
        net_final = final_price - cost - (final_price * comm_final / 100)

        # Уточнена логіка old_price_recommended
        if final_price == rozetka_price:  # Не міняємо ціну для Rozetka
            if not is_rrp_fallback:  # RRP є (price_promo_rrp), то old = price_rrp (без *1.2)
                old_price_recommended = supplier_old_price
            else:  # RRP відсутній (fallback), то old = price_rrp * 1.2
                old_price_recommended = supplier_old_price * 1.2 if supplier_old_price else final_price * 1.2
        else:  # Міняємо ціну
            old_price_recommended = final_price * 1.2

        match['recommendations'] = {
            'final_price': round(final_price, 2),
            'my_profit_target': round(my_profit_target, 2),
            'net_profit': round(net_final, 2),
            'comm_used': round(comm_final, 2),
            'used_rrp': used_rrp,
            'old_price_recommended': round(old_price_recommended, 2),
            'base_price_before_round': round(base_price, 2),
            'iterations_log': iterations_log  # НОВЕ: Лог ітерацій
        }

        logging.info(
            f"ID {match['id']}: final={final_price}, old_rec={old_price_recommended} (fallback={is_rrp_fallback}), net={net_final}")

    return matches