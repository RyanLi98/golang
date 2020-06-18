
# Event Store Prototype

This prototype event store implements a restful API for quote and policy.



## Save Usage

Use HTTP PUT or POST to create or update a particular bucket. Buckets are identified as either quote or policy and a number. 


    <PUT|POST> http://<host>:9090/<quote|policy>/<n> HTTP/1.1
    Content-Type", "application/json"

    <JSON content>

Updates need not be a complete quote or policy. Evergreen saves periodically pass changes since the last update. 

### Save Example

BiLImit and CreditScore have changed, and will be merged into quote 14 data.

POST /quote/14 HTTP/1.1
Host: localhost:9090
Content-Type: application/json; charset=UTF-8
Content-Length: 284

{
    "BiLimit": "100/300",
    "CreditScore": 750
}

## Recall Usage

Use HTTP GET to retrieve the full current state of a particular quote or policy number.

    GET http://<host>:9090/<quote|policy>/<n> HTTP/1.1
    "Accept", "application/json"

### Recall Example

    GET /policy/12 HTTP/1.1
    Host: localhost:9090
    Accept: application/json
