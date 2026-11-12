| Scenario | Fault | Alerts fired (after the fault began; cleared after it ended) | Expected, missed | Unexpected |
|---|---|---|---|---|
| baseline | 480 s | none | - | - |
| ledger-down | 183 s | **ServiceDown** (page) 89 s; 16 s; **PaymentsNotFinishing** (page) 139 s; 136 s; **LoadShedding** (ticket) 529 s; 366 s | - | - |
| kafka-down | 181 s | **OutboxLagging** (ticket) 101 s; 20 s | - | - |
| fraud-down | 242 s | **ServiceDown** (page) 87 s; 15 s; **FraudScorerFailingOpen** (ticket) 157 s; 285 s | - | - |
| webhooks-down | 420 s | **WebhooksFailing** (ticket) 328 s; 228 s | - | - |
| books-wrong | 43 s | **LedgerBooksWrong** (page) 45 s; 82 s | - | - |
| postgres-down | 121 s | **DatabaseUnreachable** (page) 49 s; 8 s; **ApiErrorBudgetBurnSlow** (ticket) 69 s; 939 s; **ApiErrorBudgetBurn** (page) 99 s; 279 s; **ApiLatencyBudgetBurn** (page) 139 s; 249 s; **LoadShedding** (ticket) 340 s; 329 s | - | - |
| slow-ledger | 901 s | **ApiLatencyBudgetBurn** (page) 504 s; 233 s; **DependencySlow** (ticket) 754 s; 163 s | - | - |
