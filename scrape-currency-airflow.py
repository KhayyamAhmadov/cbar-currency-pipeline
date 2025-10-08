from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import psycopg2
import time

# =========================
# PostgreSQL bağlantısı
# =========================
def get_conn():
    return psycopg2.connect(
        dbname='currency',
        user='postgres',
        password='1234',
        host='host.docker.internal',
        port='5432'
    )

# =========================
# Valyuta məlumatlarını çəkən funksiya
# =========================
def scrape_currency(date_to_scrape):
    conn = get_conn()
    cur = conn.cursor()

    # Cbar.az linki üçün +1 gün
    query_date = date_to_scrape + timedelta(days=1)
    date_str = query_date.strftime("%d/%m/%Y")
    url = f"https://www.cbar.az/currency/rates?date={date_str}"
    print(f"🔹 Sorğu göndərilir: {date_str} (əsl tarix: {date_to_scrape.date()})")

    r = requests.get(url)
    if r.status_code != 200:
        print(f"⚠️ {query_date.date()} üçün səhv kod: {r.status_code}")
        return

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select("div.table_row")

    for row in rows:
        name_div = row.find("div", class_="valuta")
        code_div = row.find("div", class_="kod")
        rate_div = row.find("div", class_="kurs")

        if name_div and code_div and rate_div:
            name = name_div.text.strip()
            code = code_div.text.strip().upper()
            try:
                rate = float(rate_div.text.strip().replace(",", "."))
            except ValueError:
                continue

            # ❗ BAZA ÜÇÜN HƏR ZAMAN date_to_scrape
            cur.execute("""
                INSERT INTO exchange_rates (date, code, name, rate)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date, code) DO NOTHING;
            """, (date_to_scrape.date(), code, name, rate))  # ✅ düzgün tarix

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {date_to_scrape.date()} məlumatları uğurla yazıldı.")

# =========================
# Keçmiş dataları toplamaq
# =========================
def scrape_historical_data():
    start_date = datetime(1993, 11, 25)
    end_date = datetime.today()
    current_date = start_date

    print(f"🕓 {start_date.date()} → {end_date.date()} arası tarixlər üçün məlumat yığılır...")

    while current_date <= end_date:
        try:
            scrape_currency(current_date)
        except Exception as e:
            print(f"❌ Xəta {current_date.date()}: {e}")
        time.sleep(1.5)  # saytın bloklanmaması üçün
        current_date += timedelta(days=1)

    print("✅ Keçmiş məlumatların yığılması tamamlandı!")

# =========================
# Gündəlik yenilənmə funksiyası
# =========================
def scrape_today(**kwargs):
    date_to_scrape = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    scrape_currency(date_to_scrape)

# =========================
# Airflow DAG tərifi
# =========================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'currency_scraper',
    default_args=default_args,
    schedule_interval='0 20 * * *',  # hər gün saat 20:00-da
    catchup=False,
) as dag:

    # 1️⃣ Bir dəfəlik keçmiş dataların toplanması
    task_scrape_history = PythonOperator(
        task_id='scrape_historical_data',
        python_callable=scrape_historical_data
    )

    # 2️⃣ Gündəlik yeni məlumatların çəkilməsi
    task_scrape_today = PythonOperator(
        task_id='scrape_today',
        python_callable=scrape_today,
        provide_context=True
    )

    task_scrape_history >> task_scrape_today
