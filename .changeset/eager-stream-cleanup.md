---
"pg-event-bus": patch
---

Fix `on()` and `deliveryGaps()` to subscribe immediately and buffer events before the first read.
