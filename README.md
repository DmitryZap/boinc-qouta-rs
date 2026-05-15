# boinc-quota-rs

Учебная реализация BOINC-подобной распределённой вычислительной системы на Rust.

**Что реализовано:**
- Координатор принимает задачи от клиентов и распределяет их воркерам по TCP
- Воркеры выполняют задачи в изолированных Docker-контейнерах (Python) или детерминированно симулируют результат
- Результаты проходят консенсус, взвешенный по репутации участников
- Балансы и транзакции хранятся в иммутабельном SHA-256 blockchain
- Провальные воркеры штрафуются через lease timeout механизм
- Нативный GUI на egui/eframe для обоих режимов (координатор / воркер)

---

## C4 Level 1 — System Context

```mermaid
graph TB
    coordinator["👤 Coordinator User
    —
    Создаёт проекты, задачи,
    фондирует quota,
    запускает свой executor"]

    worker["👤 Worker User
    —
    Подключается к координатору,
    выполняет задачи,
    получает токены как награду"]

    system["🦀 boinc-quota-rs
    —
    Распределённая вычислительная
    система с consensus,
    reputation и blockchain ledger"]

    docker["🐳 Docker Daemon
    —
    Изолированное выполнение
    Python кода в контейнерах
    python:3.11-alpine"]

    coordinator -->|"запускает процесс\nв режиме Coordinator"| system
    worker -->|"запускает процесс\nв режиме Worker,\nподключается по TCP"| system
    system -->|"создаёт контейнеры\nдля Python задач\n(опционально)"| docker

    classDef person fill:#08427b,color:#fff,stroke:#052e56
    classDef system_box fill:#1168bd,color:#fff,stroke:#0b4884
    classDef external fill:#999,color:#fff,stroke:#6b6b6b
    class coordinator,worker person
    class system system_box
    class docker external
```

---

## C4 Level 2 — Container Diagram

Весь процесс — один бинарник. Внутри две основные нити: GUI (главная OS-нить) и tokio async runtime.

```mermaid
graph TB
    subgraph process["boinc-quota-rs process"]
        direction TB

        subgraph gui_thread["Main OS Thread (egui)"]
            app["BoincApp
            —
            egui/eframe GUI
            5 вкладок: Connect, Projects,
            Tasks, Executor, Network
            [app.rs]"]
        end

        subgraph tokio_runtime["Tokio Async Runtime"]
            actor["NetworkActor
            —
            Мультиплексор режимов.
            Coordinator: TcpListener + select!
            Worker: TcpStream + select!
            [actor.rs]"]

            network["Network
            —
            Движок протокола:
            участники, проекты, задачи,
            консенсус, репутация,
            планировщик, leases
            [network.rs]"]

            blockchain["Blockchain
            —
            Immutable ledger.
            Mint / Transfer / Burn.
            SHA-256 hash chain.
            [blockchain.rs]"]

            executor["Executor
            —
            Выполняет задачи:
            digest computation,
            heartbeat, lease renewal,
            симуляция Byzantine поведения
            [executor.rs]"]

            compute["ComputeModule
            —
            Docker bridge:
            python:3.11-alpine,
            network=none,
            CPU/memory limits
            [compute.rs]"]

            estimator["Estimator
            —
            Оценка стоимости токенов:
            статический анализ кода,
            rusage измерения
            [estimator.rs]"]
        end
    end

    docker_ext["🐳 Docker Daemon"]
    peer["Другой узел
    (координатор или воркер)"]

    app -->|"AppCommand chan\n(mpsc, buf=64)"| actor
    actor -->|"AppEvent chan\n(mpsc, buf=256)"| app

    actor -->|"Arc<Mutex<Network>>"| network
    network -->|"owns"| blockchain

    actor -->|"spawn Executor"| executor
    executor -->|"Network::request_task\nNetwork::submit_result\nNetwork::heartbeat"| network
    executor -->|"execute_python()"| compute
    executor -->|"run_and_measure()"| estimator

    actor -->|"TCP JSON\nPeerRequest/PeerResponse\nLinesCodec"| peer
    actor -->|"broadcast chan\n→ peers"| peer

    compute -->|"Docker API\n(bollard)"| docker_ext

    classDef container fill:#1168bd,color:#fff,stroke:#0b4884
    classDef db fill:#2d6a4f,color:#fff,stroke:#1b4332
    classDef external fill:#999,color:#fff,stroke:#6b6b6b
    class app,actor,executor,compute,estimator container
    class network,blockchain db
    class docker_ext,peer external
```

---

## C4 Level 3 — Component Diagram: NetworkActor

Детализация `actor.rs` — самого сложного компонента.

```mermaid
graph TB
    subgraph actor["NetworkActor [actor.rs]"]
        direction TB

        run["run()
        —
        Ждёт первую команду:
        ConnectCoordinator → coordinator mode
        ConnectWorker → worker mode"]

        subgraph coord_mode["Coordinator Mode"]
            run_coord["run_coordinator()
            —
            Bind TcpListener,
            регистрирует себя в Network,
            tokio::select! loop"]

            handle_peer["handle_peer()
            —
            Per-connection task.
            Читает JSON строки,
            форвардит broadcast state,
            закрывает по ошибке"]

            handle_req["handle_peer_request()
            —
            Register → Welcome + broadcast
            RequestTask → TaskAssigned
            SubmitResult → ResultAck + broadcast
            Heartbeat → Ok
            GetState → StateUpdate"]

            coord_exec["coordinator_executor_loop()
            —
            spawn_blocking wrapper,
            Idle → sleep 500ms,
            ProcessedTask → broadcast"]
        end

        subgraph worker_mode["Worker Mode"]
            run_worker["run_worker()
            —
            Connect TcpStream,
            Register → Welcome,
            tokio::select! loop"]

            worker_exec["worker_executor_loop()
            —
            RequestTask → TaskAssigned,
            sleep(compute_ticks*100ms),
            SubmitResult → ResultAck,
            emit Log + StateUpdate"]
        end
    end

    cmds["AppCommand\n(from UI)"]
    events["AppEvent\n(to UI)"]
    net["Network\n(protocol engine)"]
    tcp["TCP peers"]
    bc["broadcast chan"]

    cmds --> run
    run --> run_coord
    run --> run_worker

    run_coord -->|"accept()"| handle_peer
    handle_peer --> handle_req
    handle_req -->|"lock + call"| net
    handle_req -->|"broadcast_snapshot()"| bc
    bc --> handle_peer
    bc -->|"→ peer TCP"| tcp

    run_coord -->|"StartExecutor cmd"| coord_exec
    coord_exec -->|"process_next_task()"| net
    coord_exec -->|"AppEvent::Log\nAppEvent::StateUpdate"| events

    run_worker -->|"PeerRequest/Response"| tcp
    run_worker -->|"StartExecutor cmd"| worker_exec
    worker_exec -->|"PeerRequest::RequestTask\nPeerRequest::SubmitResult"| tcp
    worker_exec -->|"AppEvent::Log\nAppEvent::StateUpdate"| events

    classDef comp fill:#1168bd,color:#fff,stroke:#0b4884
    classDef ext fill:#999,color:#fff,stroke:#6b6b6b
    class run,run_coord,handle_peer,handle_req,coord_exec,run_worker,worker_exec comp
    class cmds,events,net,tcp,bc ext
```

---

## Module Reference

| Файл | Назначение | Ключевые типы |
|------|-----------|--------------|
| [src/main.rs](src/main.rs) | Точка входа. Tokio runtime, MPSC каналы, запуск GUI и NetworkActor | `main()` |
| [src/model.rs](src/model.rs) | Доменные типы | `Participant`, `Project`, `Task`, `TaskReport`, `TaskStatus` |
| [src/protocol.rs](src/protocol.rs) | Все сообщения между компонентами | `AppCommand`, `AppEvent`, `PeerRequest`, `PeerResponse`, `NetworkSnapshot` |
| [src/blockchain.rs](src/blockchain.rs) | SHA-256 hash chain ledger | `Blockchain`, `Block`, `Transaction` |
| [src/network.rs](src/network.rs) | Движок протокола: планировщик, консенсус, репутация, leases | `Network`, `NetworkConfig`, `NetworkError` |
| [src/actor.rs](src/actor.rs) | Async networking loop, мультиплексор режимов | `NetworkActor` |
| [src/executor.rs](src/executor.rs) | Выполнение задач, digest, heartbeat | `Executor`, `ExecutorEvent` |
| [src/compute.rs](src/compute.rs) | Docker Python execution | `ComputeModule`, `ComputeResult`, `ResourceConfig` |
| [src/estimator.rs](src/estimator.rs) | Оценка стоимости токенов | `Estimator`, `CostConfig`, `ResourceMetrics` |
| [src/app.rs](src/app.rs) | egui GUI, 5 вкладок | `BoincApp`, `Tab`, `PayloadMode`, `TaskFilter` |

---

## Key Concepts

### Консенсус, взвешенный по репутации

Каждый воркер отправляет `result_digest` — строку-хэш результата. Координатор накапливает веса по репутации участников. Когда суммарный вес одного digest достигает `consensus_quorum` (по умолчанию 2), задача финализируется.

```
task.reports = [
    { worker_id: 1, digest: "abc", reputation: 3 },
    { worker_id: 2, digest: "abc", reputation: 1 },  // вес "abc" = 4 ≥ quorum → финализация
    { worker_id: 3, digest: "xyz", reputation: 2 },
]
```

Награда распределяется пропорционально репутации воркеров с правильным digest. Остаток (от деления) достаётся воркеру с наибольшей репутацией.

### Blockchain Ledger

Все движения токенов записываются в блоки:
- `Mint { to, amount }` — начальный баланс участника
- `Transfer { from, to, amount }` — перевод (fund, donate, reward)
- `Burn { from, amount }` — штраф за истёкший lease

`balance_of(address)` вычисляется пересчётом всех транзакций. Целостность цепи проверяется через SHA-256 prev_hash. Проекты имеют отдельные адреса: `project_id + 1_000_000_000`.

### Lease-Based Fault Tolerance

При назначении задачи создаётся lease с `expires_at = current_tick + lease_timeout_ticks`. Если воркер не присылает heartbeat или результат до истечения — на следующем `tick()`:
1. lease удаляется
2. репутация воркера снижается на `lease_slash`
3. баланс сжигается на `lease_slash` (Burn tx)
4. задача возвращается в очередь

### Планировщик задач

При `request_task()` выбирается задача с наивысшим приоритетом:
1. Проект с максимальным `quota_available + quota_locked` — первый
2. Внутри проекта — задача с наибольшим `task_id`
3. Воркер не может взять задачу, по которой уже отправил результат

### Docker Isolation

Python payload (`python:<base64-код>`) выполняется в контейнере:
- Image: `python:3.11-alpine` (pull при первом запуске)
- `network_mode: none` — нет исходящего интернета
- CPU shares: 512 (≈0.5 CPU)
- Memory: 256 MB
- Timeout: 10 секунд
- Контейнер удаляется принудительно после выполнения

Fallback: если Docker недоступен — системный `python3` subprocess.

### Byzantine Workers

`Executor::generates_corrupted_result(task_id, payload)` детерминированно решает, будет ли результат неправильным:

```rust
(task_id * 37 + payload_len * 11 + worker_id * 13) % 100 >= reliability_percent
```

`reliability_percent = 95` означает, что ~5% задач воркер намеренно провалит. Это позволяет тестировать консенсус с Byzantine участниками.

---

## Getting Started

### Зависимости

- Rust 1.75+
- (Опционально) Docker Desktop для выполнения Python задач

### Сборка и запуск

```bash
git clone <repo>
cd boinc-qouta-rs
cargo build --release
cargo run
```

Откроется GUI-окно 960×640.

### Python задачи (Docker)

```bash
# Убедиться что Docker запущен
docker pull python:3.11-alpine
```

---

## Quick Demo

### Шаг 1 — Запустить два процесса

Откройте два терминала и запустите `cargo run` в каждом.

### Шаг 2 — Coordinator (первый процесс)

1. Вкладка **Connect** → режим **Coordinator**
2. Listen addr: `0.0.0.0:7878`, Name: `Alice`, Balance: `100`
3. Нажать **Start Server**

### Шаг 3 — Worker (второй процесс)

1. Вкладка **Connect** → режим **Worker**
2. Coord addr: `127.0.0.1:7878`, Name: `Bob`, Balance: `50`
3. Нажать **Connect**

### Шаг 4 — Создать проект и задачу (в Coordinator)

1. Вкладка **Projects** → Create Project: `MyProject` → **Create**
2. Fund Project: ID=1, Amount=30 → **Fund**
3. Вкладка **Tasks** → выбрать проект, Reward=10, Payload: `hello-world` → **Submit**

### Шаг 5 — Запустить Executor на обоих узлах

На **Coordinator**: вкладка **Executor** → Reliability 95%, Ticks 2 → **Start**
На **Worker**: вкладка **Executor** → Reliability 95%, Ticks 2 → **Start**

> Важно: quorum=2, значит нужны результаты от обоих executor'ов для консенсуса.

Задача перейдёт в статус **Completed**. Награда распределится пропорционально репутации.

---

## Task Lifecycle

```
[Submit Task]
     │ lock reward в project quota
     ▼
  Pending  ──────────────────────────────────────────────────┐
     │ request_task()                                         │
     │ create lease (expires_at = tick + timeout)             │
     ▼                                                        │
  Assigned                                                    │
     │                                            lease expired?
     │ submit_result()                             slash + burn
     │ add TaskReport                              re-enqueue ─┘
     │
     ├── consensus reached? ──yes──► Completed
     │                               distribute reward
     │                               adjust reputation (+/-)
     │
     ├── max_reports reached, no consensus? ──► Rejected
     │                                          return quota
     │                                          penalize all
     │
     └── not enough reports yet? ──► stay Assigned
                                     wait for more workers
```

---

## Testing

```bash
cargo test
```

| Модуль | Тестов | Покрытие |
|--------|--------|---------|
| `blockchain.rs` | 12 | mint, transfer, burn, integrity, tamper detection |
| `network.rs` | 16 | task submit, consensus, rewards, lease timeout, reputation |
| `executor.rs` | 3 | reward on consensus, lease loss, idle when no tasks |
| `estimator.rs` | 5 | pre_estimate, calculate_cost, run_and_measure, determinism |

---

## Architecture Decisions

**Почему один процесс для GUI и сети?** — Простота для учебного проекта. GUI нить и tokio runtime разделены через MPSC каналы, что обеспечивает non-blocking UI при любой сетевой активности.

**Почему LinesCodec вместо бинарного протокола?** — Отладка: `tcpdump` или `netcat` показывают читаемый JSON. Для production использовать length-prefixed binary.

**Почему blockchain для балансов?** — Демонстрация аудитируемости: каждое движение токена traceable. `verify_integrity()` детектирует любую постфактум-модификацию.

**Почему reputation-weighted consensus вместо majority?** — Позволяет доверенным воркерам иметь больший вес. Новые участники (reputation=1) нуждаются в поддержке большинства.
