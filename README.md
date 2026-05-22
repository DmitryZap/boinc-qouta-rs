# boinc-quota-rs

Учебная реализация BOINC-подобной распределённой вычислительной системы на Rust.

## Сборка и запуск

Все команды через `Makefile`.

```bash
make setup   # один раз: скачать bundled Python в vendor/python
make build   # сборка release-бинарника
make run     # запустить GUI
make cli     # запустить CLI
make test    # прогнать тесты
make clean   # очистить артефакты сборки
```

`make setup` обязателен перед первой сборкой: проект линкует PyO3 против
встроенного интерпретатора python-build-standalone в `vendor/python`.
