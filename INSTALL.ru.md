# Установка коллектора nlbwmon (подготовка beta)

[English](INSTALL.md) | **Русский**

> [!CAUTION]
> **Экспериментальная предварительная версия, не гарантированный журнал аудита.** TCP между роутером и коллектором не имеет аутентификации, шифрования и прикладного подтверждения. События могут пропасть до записи в SQLite; очередь их не восстановит. Используйте доверенную сеть или защищённый туннель и сначала проверяйте на некритичных данных.

Это руководство устанавливает **новый** Python-сервис `flow_collector.py`. Он сохраняет принятые события NEW/DESTROY в SQLite WAL и отправляет обновления документов в OpenSearch через **Bulk API**. Необязательное PTR-обогащение выполняется асинхронно. **Fluent Bit для работы коллектора не нужен.**

Настройка роутера: [INSTALL форка nlbwmon](https://github.com/anideth/nlbwmon/blob/docs/bilingual-beta/INSTALL.ru.md) ([English](https://github.com/anideth/nlbwmon/blob/docs/bilingual-beta/INSTALL.md)).

## 1. Требования

- Debian-подобный Linux, systemd, Python 3 со стандартными `sqlite3` и `ssl`, постоянный диск, доступ к OpenSearch.
- OpenSearch с разрешёнными скриптовыми обновлениями и созданием индексов `conntrack-flows-*`. Правильный шаблон индексов и mapping нужны **до начала приёма событий**.
- Проверенный и **зафиксированный Git-коммит** проекта. Не обновляйтесь вслепую с движущейся ветки.
- Доверенный канал между роутером и коллектором: разрешите TCP **5517** только нужным роутерам.

Полный шаблон OpenSearch, настройки ISM и Dashboards описаны в [LIFECYCLE_SETUP.md](docs/LIFECYCLE_SETUP.md). Перед применением примеров проверьте существующие шаблоны и политики: **не перезаписывайте и не удаляйте действующие индексы/политики без анализа**. Поле времени Dashboards — `flow.started_at`. Дата индекса определяется **началом** соединения по UTC.

## 2. Установка исходников и тесты

Клонируйте репозиторий в `/opt/nlbwmon-aggregator` и переключитесь на проверенную фиксированную ревизию. Если сервис уже установлен, **не перезаписывайте рабочий каталог**: сначала запишите текущий коммит и сохраните unit и базу.

```sh
git clone https://github.com/anideth/nlbwmon-aggregator.git /opt/nlbwmon-aggregator
cd /opt/nlbwmon-aggregator && git checkout REVIEWED_COMMIT_OR_TAG
python3 -m py_compile flow_collector.py flow_lifecycle.py flow_opensearch.py flow_writer.py dns_resolver.py && python3 -m unittest -v test_dns_resolver test_flow_lifecycle test_flow_opensearch test_flow_writer test_flow_collector
```

## 3. Учётная запись, хранение и секреты

Выполните на **сервере коллектора** от root:

```sh
id flow-collector >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin flow-collector
getent group flow-secrets >/dev/null || groupadd --system flow-secrets
usermod -aG flow-secrets flow-collector
install -d -o flow-collector -g flow-collector -m 750 /opt/nlbwmon-data
install -d -o root -g flow-secrets -m 750 /etc/flow-writer
```

Создайте `/etc/flow-writer/password`, **не помещая пароль в Git, историю shell, команды и журналы**. Владелец `root`, группа `flow-secrets`, права `0640`. Для OpenSearch используйте отдельную учётную запись с минимальными правами на `conntrack-flows-*`. При собственной CA укажите её сертификат, не отключая проверку TLS. Пример использует Basic Auth; код также поддерживает ограниченный API key.

## 4. Настройка и запуск systemd

Скопируйте [examples/flow-collector.service](examples/flow-collector.service) в `/etc/systemd/system/flow-collector.service`. Откройте через `vim` и замените **все** заглушки: IP прослушивания, URL OpenSearch, путь к CA, имя пользователя и путь к секрету. `192.0.2.10` и `search.example` в шаблоне — **демонстрационные адреса**, они не заработают как есть. Не открывайте TCP 5517 в Интернет.

```sh
cp /opt/nlbwmon-aggregator/examples/flow-collector.service /etc/systemd/system/flow-collector.service && vim /etc/systemd/system/flow-collector.service
systemd-analyze verify /etc/systemd/system/flow-collector.service && systemctl daemon-reload && systemctl enable --now flow-collector.service
systemctl status flow-collector.service; journalctl -u flow-collector.service -n 100 --no-pager
```

Пример включает `--dns` (необязательный PTR) и `--password-file`. Значения по умолчанию в коде: `--bulk-size 100` и `--max-events 100000`; изменяйте их только после измерения очереди и скорости OpenSearch. **Не запускайте два экземпляра с одной SQLite-базой.** Сервис записывает данные только в `/opt/nlbwmon-data`.

## 5. Подключение OpenWrt и проверка

На роутере настройте существующую UCI-секцию `flow_export`: уникальный `router_name`, адрес коллектора и **TCP-порт 5517**. Включайте экспорт только после запуска приёмника и настройки firewall. Учитывайте особенности `mark/mask` из [INSTALL nlbwmon](https://github.com/anideth/nlbwmon/blob/docs/bilingual-beta/INSTALL.ru.md).

Проверьте прослушивание порта и очередь:

```sh
ss -ltn | grep ':5517'
python3 -c 'import sqlite3; c=sqlite3.connect("file:/opt/nlbwmon-data/flows.db?mode=ro",uri=True); print("events",c.execute("SELECT COUNT(*) FROM events").fetchone()[0],"dns_jobs",c.execute("SELECT COUNT(*) FROM dns_jobs").fetchone()[0]); c.close()'
```

В OpenSearch Dashboards откройте `conntrack-flows-*` с полем времени `flow.started_at`. Создайте тестовое соединение: NEW создаёт документ `flow.status=active`, DESTROY обновляет **тот же документ** до `closed` с итоговыми счётчиками. Если DESTROY потерян, `active` может остаться навсегда. Следите за ошибками в journal и размером очереди SQLite: рост очереди может означать недоступность OpenSearch, низкую скорость записи или ошибки авторизации/mapping. PTR при включении создаёт дополнительные обновления OpenSearch.

## 6. Обновление, откат и подготовка к публикации

Перед обновлением запишите `git rev-parse HEAD`, сохраните unit и **согласованную копию SQLite** (с учётом WAL), зафиксируйте mapping OpenSearch. Перед обновлением файлов остановите `flow-collector.service`. Переключитесь на проверенную ревизию, выполните тесты и перезапустите сервис. Для отката верните предыдущие ревизию/unit **без удаления накопленных событий**; проверьте совместимость схемы.

Дополнительные проверки и резервирование: [FLOW_COLLECTOR_DEPLOYMENT.md](docs/FLOW_COLLECTOR_DEPLOYMENT.md). В старой документации может упоминаться поштучная отправка в OpenSearch; в коде подготовки beta используется **Bulk API**.

**Перед открытием репозитория:** проверьте все файлы, примеры, конфигурации и **историю Git** на пароли, приватные IP, имена хостов, персональные данные и информацию работодателя; при обнаружении секретов замените их. Проверьте авторство и добавьте подходящую лицензию, испытайте установку на чистой VM, откат и всю цепочку роутер → SQLite → OpenSearch. **Не открывайте репозиторий и не публикуйте релиз до этой проверки.**

## Изолированная тестовая среда

**Изолированные тесты на существующем OpenSearch:** для тестового коллектора укажите `--index-prefix conntrack-flows-tests` (по умолчанию остаётся `conntrack-flows`). Это направляет **и Bulk-записи, и асинхронные PTR-обновления** в `conntrack-flows-tests-YYYY.MM.DD`. До подключения VM создайте отдельный шаблон индекса и учётную запись с минимальными правами только на `conntrack-flows-tests-*`. Предпочтительна отдельная тестовая учётная запись с минимальными правами. Если лаборатория использует существующую учётную запись записи, зафиксируйте исключение и проверьте изоляцию индексов; рабочие шаблоны не меняйте.
