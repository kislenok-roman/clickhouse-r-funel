# Motivation
Try to create funel using our access_log structure and ClickHouse. There are a great function [sequenceMatch](https://clickhouse.yandex/reference_en.html#sequenceMatch(pattern)(time%2C%20cond1%2C%20cond2%2C%20...)), but we have some problems using it:

* There are no way one can match a row for differrent events; ex this should match the same row in log:

```
step1: 
url like '/apply/payment%'

step2:
url like '/apply/payments/using/card%'
```

* It fails a lot (A LOT) for us because of it's inner limitations (1 000 000 iterations per session)

I try to use joins instead with some smart (I want to think so) sampling.
