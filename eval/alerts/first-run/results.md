| Scenario | Fault | Alerts fired (after the fault began; cleared after it ended) | Expected, missed | Also fired |
|---|---|---|---|---|
| baseline | 480 s | none | - | - |
| ledger-down | 182 s | **ServiceDown** (page) 78 s; 16 s; **PaymentsNotFinishing** (page) 138 s; 156 s | - | - |
| kafka-down | 182 s | **LoadShedding** (ticket) 99 s; 0 s | OutboxLagging | LoadShedding |
| fraud-down | 241 s | **ServiceDown** (page) 86 s; 15 s; **FraudScorerFailingOpen** (ticket) 156 s; 266 s; **LoadShedding** (ticket) 426 s; 205 s | - | LoadShedding |
| slow-ledger | 361 s | **LoadShedding** (ticket) 341 s; 300 s | ApiLatencyBudgetBurn | LoadShedding |
| postgres-down | 121 s | none | ApiErrorBudgetBurn | - |
| webhooks-down | 420 s | **LoadShedding** (ticket) 210 s; 0 s; **WebhooksFailing** (ticket) 340 s; 951 s | - | LoadShedding |
| books-wrong | 85 s | **WebhooksFailing** (ticket) 8 s; 223 s; **LedgerBooksWrong** (page) 78 s; 63 s | - | WebhooksFailing |
