# Overview

A simple asynchronous web crawler framework implemented using `httpx`.

## Architecture

```mermaid
graph TD
    A[Spider] --> B[Scheduler]
    B --> C[Downloader]
    C --> D[Parser]
    D --> B
    D --> A

 ```

## Main Data Flow

1. Spider initializes requests -> Scheduler
2. Scheduler dispatches requests -> Downloader
3. Downloader retrieves responses -> Parser
4. Parser processes data to generate new requests -> Scheduler
5. Parser processes data to generate results -> Spider
