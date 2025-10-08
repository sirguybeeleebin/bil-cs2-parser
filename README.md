# 🕹️ bil-cs2-parser

**bil-cs2-parser** — это утилита для извлечения, нормализации и публикации данных матчей CS2 (Counter-Strike 2).  
Программа читает «сырые» JSON-файлы матчей, превращает их в структурированный формат (player × opponent × round)  
и сохраняет в отдельную директорию. После завершения обработки отправляет уведомление в RabbitMQ.

---

## 🚀 Основные возможности

- 📂 Чтение матчей из директории `games_raw`
- 🧩 Преобразование данных в плоский формат (flatten)
- 💾 Сохранение результатов в `games_flatten`
- 📨 Публикация события в RabbitMQ
- ⚙️ Настройка через `.env` или CLI аргумент `--env-file`
- 🧠 Логирование с настраиваемым уровнем (`DEBUG`, `INFO`, и т.д.)

---

## ⚙️ Запуск

Создайте файл `.env` в корне проекта:

```env
APP_LOG_LEVEL=INFO
RABBITMQ_URL=amqp://guest:guest@localhost/
RABBITMQ_QUEUE_NAME=games_flatten
GAMES_RAW_DIR=../bil-cs2-data/games_raw
GAMES_FLATTEN_DIR=../bil-cs2-data/games_flatten
```

Затем запустите скрипт с указанием пути к .env через флаг --env-file:

```bash
poetry install
poetry run main.py --env-file .env
```

В корне проекта (где находится `Dockerfile`) выполните команду:

### Docker

```bash
docker build -t bil-cs2-parser .
````

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/.env:/app/.env \
  --name bil-parser \
  bil-cs2-parser
```