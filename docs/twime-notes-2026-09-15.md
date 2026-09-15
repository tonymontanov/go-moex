# TWIME: факты протокола и решения кодека (15.09.2026)

Источники (все публичные, `ftp.moex.com/pub/TWIME/Spectra/prod/`):
`doc/spectra_twime_en.pdf` (документ 9.9.0 от 10.07.2026, 69 стр.),
`doc/twime_spectra-7.7.xml` (schemaId 19781, version 7),
`doc/Twime connection guide.pdf`, `doc/TWIME FAQ 1.0.1.pdf` (2016, частично устарел),
`samples/twime_certification.zip` (Python-скрипты шагов сертификации),
`samples/WireClient-7.0.zip` (эталонный Python-клиент). Регламент сертификации
в архиве отсутствует — его выдаёт help@moex.com.

Реализация: `internal/sbe` (общие SBE-примитивы), `internal/twime`
(`schema.go`, `messages.go`, `encode_server.go`, `codec.go`, `pacer.go`,
`session.go`, `metrics.go`).

## Провод

- Кадр = 8-байтовый SBE `messageHeader` (blockLength, templateId, schemaId,
  version; uint16 LE) + `blockLength` байт root block. Префикса длины (SOFH)
  нет; `blockLength` не включает заголовок. Выравнивание 1 байт, паддинга
  нет (FAQ §3.2; сериализатор биржи — `struct('<...')`).
- Char-поля (`String7/20/25`) добиваются NUL. Decimal5 — только int64
  mantissa, экспонента −5 константная и на провод не попадает.
- Все `Int*/UInt*` optional с `nullValue = max+1`; «не задано» кодируется
  сентинелом, не нулём (например `ExpireDate = NullTimestamp`,
  `SecurityID = NullInt32` в mass cancel).
- В XML 7.0 и 7.7 версия схемы одна (`version="7"`); разница — удалены
  `IntradayClearing*` из `TradSesEventEnum`. blockLength всех 28 сообщений
  проверены против `schema.py` биржи (golden-тест).
- Расхождение текста спеки и XML: `SecurityTypeSet/Multileg` в тексте «4», в
  XML `<choice>2</choice>` (бит 2 → маска 0b100). Кодек берёт XML; это же
  использует `cert_2_7.py` (`0b100 # bitmask for multilegs`).

## Сессия

- Establish в течение 10 с после TCP-коннекта. `Credentials` = логин
  (String20), пароля нет — аутентификация логин + IP отправителя.
- `KeepaliveInterval` 1000..60000 мс; в `EstablishmentAck` сервер сообщает
  **свой** интервал. Клиент обязан прислать любое сообщение внутри своего
  интервала (иначе `Terminate(MissedHeartbeat)` через 1–2 интервала); сервер
  шлёт что-то не реже своего. Heartbeat = `Sequence`; от клиента с
  `NextSeqNo = nullValue`, от сервера — с номером следующего прикладного
  сообщения. Не более 3 heartbeat/с (4-й → `Terminate(TooFastClient)`),
  рекомендуемый шаг ≥ 600 мс.
- Нумеруются только сообщения сервер→клиент **прикладного** уровня
  (template ≥ 6000); сессионные (5xxx: FloodReject, SessionReject,
  BusinessMessageReject, Sequence…) номера не имеют и не ретранслируются.
  У сообщений клиент→сервер номеров нет — только `ClOrdID`.
- `RetransmitRequest` на транзакционном шлюзе — ≤ 10 сообщений за запрос,
  `Retransmission` приходит примерно через секунду, за ней ровно `Count`
  прикладных кадров; в это время сервер не шлёт живые сообщения. Отдельный
  recovery-шлюз (тот же логин, другой порт) отдаёт до 1000 за запрос — не
  реализован.
- Сообщения чистятся между 01:00 и 05:00 MSK, счётчик сбрасывается: новый
  `NextSeqNo` может быть меньше прежнего — сессия принимает серверное
  значение без recovery.
- Terminate — рукопожатие: инициатор ждёт ответный `Terminate` (ранний
  разрыв TCP теряет `ExecutionSingleReport` в полёте). Сервер после своего
  `Terminate` закрывает TCP сам.
- Реконнект: Establish и TCP SYN не чаще 1 раза в секунду
  (`EstablishmentReject(TooFastReconnect)`). Два соединения с одним логином
  → обе сессии рвутся с `AlreadyEstablished`.
- COD (cancel-on-disconnect) включается заявкой в бирже, в протоколе
  настройки нет; снятые по COD заявки приходят `OrderCancelResponse` с
  флагом `COD` (бит 32) при следующем подключении.

## Лимиты и отказы

- Бюджет торговых сообщений (`NewOrderSingle`, `OrderCancelRequest`,
  `OrderReplaceRequest`, `OrderMassCancelRequest` и айсберг-варианты):
  30 × единицы производительности логина в секунду (до 3000). Считается по
  фиксированной секунде на стороне шлюза. Превышение → `FloodReject`
  (`QueueSize` = принято за последнюю секунду, `PenaltyRemain` — **микро**секунды
  паузы), устойчивое превышение ×2 → `Terminate(TooFastClient)`.
  Пейсер на клиенте — скользящее окно 1 с, что строже любой фиксированной
  секунды. Пайплайнинг N сообщений в один write скидки не даёт.
- `SessionReject` — ошибки формата/значений (`RefTagID` = FIX-тег поля),
  в т.ч. `ClOrdIdIsNotUnique` (101): `ClOrdID` уникален в рамках торговой
  сессии для дневных и на весь срок для многодневных заявок (FAQ 2016
  утверждает обратное — верить спеке 2026).
- `BusinessMessageReject` — отказ прикладной логики, `OrdRejReason` из
  «List of return codes» спеки (~380 кодов; в кодек не перенесены).
- `OrderCancelResponse` с `ClOrdID = nullValue` — незапрошенное снятие
  (клиринг, COD, UKS, кросс, `Mode=2/3` при replace).
- `OrderReplaceRequest.Mode`: 0 объём не менять, 1 заменить объём,
  2 если объём не совпал — снять, 3 FIX-style (новый объём минус исполненное).
  Replace даёт новый `OrderID`, старый — в `PrevOrderID`.
- `OrderMassCancelRequest` — одна транзакция; на каждую снятую заявку
  приходит `OrderCancelResponse`, затем `OrderMassCancelResponse` с
  `TotalAffectedOrders`. Фильтры: `ClOrdLinkID ≠ 0` перекрывает остальные;
  `SecurityID` 0/null = любой; `Side = AllOrders (89)`; `Account` с `%%%`
  в конце = все счета логина; `SecurityGroup` пустой или `%` = все.
- `EmptyBook` после старта основного клиринга: все заявки сессии уже сняты,
  слать отмены на них нельзя (будут отклонены и считаются ошибочными).
- `TimeInForce`: Day 0, IOC 3, FOK 4, GTD 6 (+ `ExpireDate`), BOC 122 —
  book-or-cancel, то есть post-only. Алгоритмические заявки помечаются
  `ComplianceID = 'R'`.

## Что не проверено (ждёт G1/G2)

- Адреса/порты прод- и тест-контура (в connection guide только резервный
  ЦОД M1: транзакционный 91.203.254.32:9000, recovery :9001).
- Реальные тайминги `Retransmission`, поведение recovery-шлюза, коды
  `OrdRejReason` в ответ на конкретные ошибки.
- Регламент сертификации (ожидаемые ответы и флаги по шагам).
- Что делать после `Terminate(ServerShutdown/SequenceReset)` кроме
  реконнекта с принятием серверного `NextSeqNo` — в спеке не описано.
