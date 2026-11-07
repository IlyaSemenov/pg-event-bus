---
"pg-event-bus": minor
---

Send events of up to about 1 MB instead of failing above the 8000-byte PostgreSQL notification limit; listeners on earlier versions discard such large events.
